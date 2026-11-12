//! A cgroup v2 leaf per attempt (v30 part 3), runner side: enter `<unit>/attempt-<id>` before the
//! job's preparation, sample it while the job runs, and after the job and its cleanup read what
//! the kernel measured, kill leftovers and remove it. The coordinator prepares `<unit>` and
//! names it on this runner's argv (`--cgroup UNIT`, or `--cgroup-unavailable WHY`); the layout
//! and the shared types are in `patchweft_control::cgroup`.
//!
//! Measurement only: nothing here sets `memory.max` or any other limit. No failure here may fail or
//! refuse a job: every path that cannot measure ends in `{"unavailable": why}` and the job runs as
//! it would without cgroups.
//!
//! Self-contained (std, libc and serde_json only); nothing of the scheduler reaches it.

use patchweft_control::cgroup::{COORDINATOR, parse_self_cgroup};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fs, io,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// How often a running attempt's `memory.stat` is read.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
/// `anon + shmem + kernel`: memory that stays charged while the job runs, unlike page cache.
pub const NONCACHE_KERNEL: &str = "anon + shmem + kernel";
/// The same on kernels whose `memory.stat` has no `kernel` key.
pub const NONCACHE_PARTS: &str = "anon + shmem + kernel_stack + slab + percpu + sock";
const CGROUP2_SUPER_MAGIC: i64 = 0x6367_7270;

/// Whether `path` is on a cgroup2 filesystem.
pub fn is_cgroup2(path: &Path) -> bool {
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    // `f_type`'s width differs between architectures.
    #[allow(clippy::unnecessary_cast)]
    unsafe {
        libc::statfs(path.as_ptr(), &mut stat) == 0 && stat.f_type as i64 == CGROUP2_SUPER_MAGIC
    }
}

/// Supervisor-side settings; `Config::default()` is production.
#[derive(Debug, Clone)]
pub struct Config {
    pub interval: Duration,
    /// How long `finish` waits for killed leftovers to leave before removing the cgroup.
    pub kill_wait: Duration,
    /// Refuse a unit path that is not on cgroup2; off only for a test's fake tree.
    pub require_cgroup2: bool,
    /// `/proc/self/cgroup`, where `finish` confirms it left the attempt's cgroup before killing
    /// what is in it; a test's fake tree names its own.
    pub self_cgroup: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            interval: SAMPLE_INTERVAL,
            kill_wait: Duration::from_secs(2),
            require_cgroup2: true,
            self_cgroup: PathBuf::from("/proc/self/cgroup"),
        }
    }
}

/// The non-cache running total of what the sampler saw.
#[derive(Debug)]
struct Track {
    reservation_bytes: Option<u64>,
    peak: Option<(u64, Instant)>,
    formula: Option<&'static str>,
    above: Duration,
    last: Option<Instant>,
    samples: u64,
    command_started: Option<Instant>,
}

impl Track {
    fn new(reservation_mb: u32) -> Self {
        Self {
            reservation_bytes: (reservation_mb > 0).then(|| u64::from(reservation_mb) << 20),
            peak: None,
            formula: None,
            above: Duration::ZERO,
            last: None,
            samples: 0,
            command_started: None,
        }
    }

    /// At or above 90% of the reservation.
    fn high(&self, value: u64) -> bool {
        self.reservation_bytes
            .is_some_and(|reserved| u128::from(value) * 10 >= u128::from(reserved) * 9)
    }

    /// One sample. A sample at or above 90% counts the time since the previous sample, so the
    /// count is exact to one interval.
    fn record(&mut self, value: u64, formula: &'static str, at: Instant) {
        if self.high(value)
            && let Some(last) = self.last
        {
            self.above += at.saturating_duration_since(last);
        }
        self.last = Some(at);
        if self.peak.is_none_or(|(peak, _)| value > peak) {
            self.peak = Some((value, at));
        }
        self.formula = Some(formula);
        self.samples += 1;
    }
}

/// Keyed `name value` lines: `memory.stat`, `memory.events`, `cpu.stat`.
pub fn parse_flat_keyed(text: &str) -> BTreeMap<String, u64> {
    text.lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(' ')?;
            Some((key.to_owned(), value.trim().parse().ok()?))
        })
        .collect()
}

/// `total=` of a pressure file's `some` or `full` line, in microseconds.
pub fn parse_pressure_total(text: &str, kind: &str) -> Option<u64> {
    text.lines()
        .find(|line| line.split_whitespace().next() == Some(kind))?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("total="))?
        .parse()
        .ok()
}

/// Non-cache bytes of one `memory.stat` reading, and the formula used.
pub fn noncache(stat: &BTreeMap<String, u64>) -> Option<(u64, &'static str)> {
    let anon = *stat.get("anon")?;
    let get = |key: &str| stat.get(key).copied().unwrap_or(0);
    Some(match stat.get("kernel") {
        Some(kernel) => (anon + get("shmem") + kernel, NONCACHE_KERNEL),
        None => (
            anon + get("shmem") + get("kernel_stack") + get("slab") + get("percpu") + get("sock"),
            NONCACHE_PARTS,
        ),
    })
}

fn write(path: &Path, text: &str) -> io::Result<()> {
    fs::write(path, text)
}

fn read_u64(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn pids(path: &Path) -> Vec<u32> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

fn sample(directory: &Path, track: &Mutex<Track>) {
    let Ok(text) = fs::read_to_string(directory.join("memory.stat")) else {
        return;
    };
    if let Some((value, formula)) = noncache(&parse_flat_keyed(&text)) {
        let mut track = track.lock().unwrap_or_else(PoisonError::into_inner);
        track.record(value, formula, Instant::now());
    }
}

/// An attempt's cgroup, entered by its supervisor, sampled until `finish`.
pub struct Attempt {
    unit: PathBuf,
    directory: PathBuf,
    pid: u32,
    config: Config,
    entered: Instant,
    reservation_mb: u32,
    track: Arc<Mutex<Track>>,
    stop: Option<mpsc::Sender<()>>,
    sampler: Option<JoinHandle<()>>,
}

/// Supervisor side, before the job's preparation: create `<unit>/attempt-<id>`, move this process
/// into it and start sampling. `reservation_mb` is the job's `memory_mb`, the 90% mark's base.
pub fn enter_attempt(unit: &Path, attempt: &str, reservation_mb: u32) -> Result<Attempt, String> {
    enter_attempt_with(unit, attempt, reservation_mb, Config::default())
}

pub fn enter_attempt_with(
    unit: &Path,
    attempt: &str,
    reservation_mb: u32,
    config: Config,
) -> Result<Attempt, String> {
    let safe = !attempt.is_empty()
        && !attempt.starts_with('.')
        && attempt
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b));
    if !safe {
        return Err(format!("attempt id {attempt:?} is not a cgroup name"));
    }
    if !unit.is_absolute() {
        return Err(format!("{} is not absolute", unit.display()));
    }
    if config.require_cgroup2 && !is_cgroup2(unit) {
        return Err(format!("{} is not on a cgroup2 filesystem", unit.display()));
    }
    let directory = unit.join(format!("attempt-{attempt}"));
    fs::create_dir(&directory).map_err(|e| format!("create {}: {e}", directory.display()))?;
    let pid = std::process::id();
    if let Err(e) = write(&directory.join("cgroup.procs"), &pid.to_string()) {
        let _ = remove(&directory);
        return Err(format!("enter {}: {e}", directory.display()));
    }
    let track = Arc::new(Mutex::new(Track::new(reservation_mb)));
    let (stop, stopped) = mpsc::channel::<()>();
    let sampler = {
        let (directory, track, interval) = (directory.clone(), Arc::clone(&track), config.interval);
        thread::Builder::new()
            .name("cgroup-sampler".into())
            .spawn(move || {
                loop {
                    sample(&directory, &track);
                    match stopped.recv_timeout(interval) {
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        _ => break,
                    }
                }
            })
    };
    // Without a sampler the end-of-attempt counters still stand.
    let sampler = sampler.ok();
    Ok(Attempt {
        unit: unit.to_path_buf(),
        directory,
        pid,
        config,
        entered: Instant::now(),
        reservation_mb,
        track,
        stop: Some(stop),
        sampler,
    })
}

fn remove(directory: &Path) -> io::Result<()> {
    if is_cgroup2(directory) {
        fs::remove_dir(directory)
    } else {
        // A test's fake tree holds regular files; a real cgroup's files go with rmdir.
        fs::remove_dir_all(directory)
    }
}

fn signed_seconds(at: Instant, origin: Instant) -> f64 {
    if at >= origin {
        (at - origin).as_secs_f64()
    } else {
        -(origin - at).as_secs_f64()
    }
}

impl Attempt {
    /// The attempt's cgroup directory.
    #[cfg(test)]
    pub fn path(&self) -> &Path {
        &self.directory
    }

    /// The job's command starts now: `noncache_peak_at_seconds` counts from here.
    pub fn command_started(&self) {
        let mut track = self.track.lock().unwrap_or_else(PoisonError::into_inner);
        track.command_started.get_or_insert_with(Instant::now);
    }

    fn stop_sampler(&mut self) {
        drop(self.stop.take());
        if let Some(sampler) = self.sampler.take() {
            let _ = sampler.join();
        }
    }

    /// After the job's processes and its cleanup are gone: read the counters, kill and record
    /// what is left, leave the cgroup and remove it. The fields, as one JSON object.
    pub fn finish(mut self) -> Value {
        self.stop_sampler();
        sample(&self.directory, &self.track);
        let directory = self.directory.clone();
        let at = |name: &str| directory.join(name);
        let mut out = Map::new();
        out.insert(
            "memory_peak_bytes".into(),
            json!(read_u64(&at("memory.peak"))),
        );
        out.insert(
            "swap_peak_bytes".into(),
            json!(read_u64(&at("memory.swap.peak"))),
        );
        {
            let track = self.track.lock().unwrap_or_else(PoisonError::into_inner);
            let origin = track.command_started.unwrap_or(self.entered);
            out.insert("noncache_peak_bytes".into(), json!(track.peak.map(|p| p.0)));
            out.insert(
                "noncache_peak_at_seconds".into(),
                json!(track.peak.map(|(_, when)| signed_seconds(when, origin))),
            );
            out.insert("noncache_formula".into(), json!(track.formula));
            out.insert(
                "seconds_above_90pct".into(),
                json!(track.reservation_bytes.map(|_| track.above.as_secs_f64())),
            );
            out.insert("samples".into(), json!(track.samples));
        }
        out.insert(
            "sample_seconds".into(),
            json!(self.config.interval.as_secs_f64()),
        );
        out.insert("reservation_mb".into(), json!(self.reservation_mb));
        let events = fs::read_to_string(at("memory.events"))
            .ok()
            .map(|text| parse_flat_keyed(&text));
        for key in ["oom", "oom_kill"] {
            out.insert(key.into(), json!(events.as_ref().and_then(|e| e.get(key))));
        }
        let cpu = fs::read_to_string(at("cpu.stat"))
            .ok()
            .and_then(|text| parse_flat_keyed(&text).get("usage_usec").copied());
        out.insert("cpu_usec".into(), json!(cpu));
        out.insert("pids_peak".into(), json!(read_u64(&at("pids.peak"))));
        let pressure = |file: &str, kind: &str| {
            fs::read_to_string(at(file))
                .ok()
                .and_then(|text| parse_pressure_total(&text, kind))
        };
        out.insert(
            "memory_pressure_full_usec".into(),
            json!(pressure("memory.pressure", "full")),
        );
        out.insert(
            "cpu_pressure_some_usec".into(),
            json!(pressure("cpu.pressure", "some")),
        );
        out.insert("includes_supervisor".into(), json!(true));
        let mut errors = Vec::new();
        let leftovers: Vec<u32> = pids(&at("cgroup.procs"))
            .into_iter()
            .filter(|pid| *pid != self.pid)
            .collect();
        // Out first: `cgroup.kill` kills every process in the cgroup, this one included.
        let leaf = self.unit.join(COORDINATOR).join("cgroup.procs");
        let left = match write(&leaf, &self.pid.to_string()) {
            Ok(()) => self.left(),
            Err(e) => {
                errors.push(format!("leave for {}: {e}", leaf.display()));
                false
            }
        };
        if !leftovers.is_empty() {
            out.insert(
                "leftover_pids".into(),
                json!({"count":leftovers.len(),"pids":&leftovers[..leftovers.len().min(10)]}),
            );
            if left {
                self.kill(&leftovers, &mut errors);
            } else {
                // Still inside: `cgroup.kill` would end this supervisor and its result with it.
                errors.push(format!(
                    "still in {}: leftovers killed by pid",
                    directory.display()
                ));
                kill_each(&leftovers);
            }
            let remaining = self.wait_empty(&leftovers);
            if remaining > 0 {
                out.insert("leftover_after_kill".into(), json!(remaining));
            }
        }
        if let Err(e) = self.remove_patiently() {
            errors.push(format!("remove {}: {e}", directory.display()));
        }
        if !errors.is_empty() {
            out.insert("errors".into(), json!(errors));
        }
        Value::Object(out)
    }

    /// Whether this process's own cgroup, read again, is no longer the attempt's. Unreadable
    /// counts as still inside.
    fn left(&self) -> bool {
        let Some(name) = self.directory.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        fs::read_to_string(&self.config.self_cgroup)
            .ok()
            .and_then(|text| parse_self_cgroup(&text).map(|own| own.to_owned()))
            .is_some_and(|own| Path::new(&own).file_name() != Some(name.as_ref()))
    }

    /// Only once this process has left the attempt's cgroup ([`Attempt::left`]).
    fn kill(&self, leftovers: &[u32], errors: &mut Vec<String>) {
        let file = self.directory.join("cgroup.kill");
        if let Err(e) = write(&file, "1") {
            // Kernels before 5.14 have no cgroup.kill: the pids just read, one by one.
            errors.push(format!("{}: {e}; killed by pid", file.display()));
            kill_each(leftovers);
        }
    }

    /// Reaps any killed leftover that is this process's child (it is the subreaper) and waits for
    /// the cgroup to empty. The pids still in it after `kill_wait`.
    fn wait_empty(&self, leftovers: &[u32]) -> usize {
        let started = Instant::now();
        loop {
            for pid in leftovers {
                unsafe {
                    libc::waitpid(*pid as i32, std::ptr::null_mut(), libc::WNOHANG);
                }
            }
            let remaining = pids(&self.directory.join("cgroup.procs"))
                .into_iter()
                .filter(|pid| *pid != self.pid)
                .count();
            if remaining == 0 || started.elapsed() >= self.config.kill_wait {
                return remaining;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn remove_patiently(&self) -> io::Result<()> {
        let started = Instant::now();
        loop {
            match remove(&self.directory) {
                Err(e)
                    if e.raw_os_error() == Some(libc::EBUSY)
                        && started.elapsed() < self.config.kill_wait =>
                {
                    thread::sleep(Duration::from_millis(20));
                }
                other => return other,
            }
        }
    }
}

/// SIGKILL to each of `leftovers`, which never holds this process's own pid.
fn kill_each(leftovers: &[u32]) {
    for pid in leftovers {
        unsafe {
            libc::kill(*pid as i32, libc::SIGKILL);
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.stop_sampler();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAT: &str = "anon 104857600\nfile 2147483648\nkernel 8388608\nkernel_stack 1048576\n\
        pagetables 2097152\nsec_pagetables 0\npercpu 4096\nsock 0\nvmalloc 0\nshmem 1048576\n\
        file_mapped 0\nslab 4194304\nslab_reclaimable 3145728\npgfault 12345\n";
    const STAT_OLD: &str = "anon 1000\nfile 99999\nkernel_stack 10\nslab 200\npercpu 3\n\
        sock 4\nshmem 50\n";
    const EVENTS: &str = "low 0\nhigh 0\nmax 3\noom 2\noom_kill 1\noom_group_kill 0\n";
    const CPU: &str = "usage_usec 7654321\nuser_usec 6000000\nsystem_usec 1654321\n\
        nr_periods 0\nnr_throttled 0\nthrottled_usec 0\n";
    const PRESSURE: &str = "some avg10=0.00 avg60=1.50 avg300=0.20 total=123456\n\
        full avg10=0.00 avg60=0.10 avg300=0.01 total=7890\n";
    const CPU_PRESSURE_OLD: &str = "some avg10=2.00 avg60=1.00 avg300=0.50 total=999\n";

    #[test]
    fn parses_memory_stat_and_both_noncache_formulas() {
        let stat = parse_flat_keyed(STAT);
        assert_eq!(stat["anon"], 104_857_600);
        assert_eq!(stat["slab_reclaimable"], 3_145_728);
        assert_eq!(
            noncache(&stat),
            Some((104_857_600 + 1_048_576 + 8_388_608, NONCACHE_KERNEL)),
            "page cache (file) is never counted"
        );
        let old = parse_flat_keyed(STAT_OLD);
        assert_eq!(
            noncache(&old),
            Some((1000 + 50 + 10 + 200 + 3 + 4, NONCACHE_PARTS))
        );
        assert_eq!(
            noncache(&parse_flat_keyed("file 5\n")),
            None,
            "no anon: no reading"
        );
    }

    #[test]
    fn parses_events_cpu_and_pressure() {
        let events = parse_flat_keyed(EVENTS);
        assert_eq!((events["oom"], events["oom_kill"]), (2, 1));
        assert_eq!(parse_flat_keyed(CPU)["usage_usec"], 7_654_321);
        assert_eq!(parse_pressure_total(PRESSURE, "full"), Some(7890));
        assert_eq!(parse_pressure_total(PRESSURE, "some"), Some(123_456));
        // cpu.pressure has no `full` line on kernels before 5.13.
        assert_eq!(parse_pressure_total(CPU_PRESSURE_OLD, "some"), Some(999));
        assert_eq!(parse_pressure_total(CPU_PRESSURE_OLD, "full"), None);
        assert_eq!(parse_pressure_total("", "some"), None);
    }

    #[test]
    fn counts_time_at_or_above_ninety_percent_and_the_peak() {
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let mib = 1u64 << 20;
        // 1000 MiB reserved: 900 MiB is the mark, inclusive.
        let mut track = Track::new(1000);
        track.record(100 * mib, NONCACHE_KERNEL, at(0));
        track.record(900 * mib, NONCACHE_KERNEL, at(5)); // at the mark: counts 5 s
        track.record(900 * mib - 1, NONCACHE_KERNEL, at(10)); // just below: nothing
        track.record(950 * mib, NONCACHE_KERNEL, at(15)); // counts 5 s, the peak
        track.record(950 * mib, NONCACHE_KERNEL, at(20)); // equal, not a new peak; counts 5 s
        track.record(10 * mib, NONCACHE_KERNEL, at(25));
        assert_eq!(track.above, Duration::from_secs(15));
        assert_eq!(
            track.peak,
            Some((950 * mib, at(15))),
            "the first time of the peak"
        );
        assert_eq!(track.samples, 6);
        // 899.9 MiB of 1000 is below 90%.
        let mut track = Track::new(1000);
        track.record(0, NONCACHE_KERNEL, at(0));
        track.record(899 * mib, NONCACHE_KERNEL, at(5));
        assert_eq!(track.above, Duration::ZERO);
        // No reservation: no mark.
        let mut track = Track::new(0);
        track.record(0, NONCACHE_KERNEL, at(0));
        track.record(u64::MAX, NONCACHE_KERNEL, at(5));
        assert_eq!(
            (track.reservation_bytes, track.above),
            (None, Duration::ZERO)
        );
    }

    /// A fake cgroup tree: `<root>/<unit>` with the files a coordinator writes.
    fn fake_unit() -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let unit = temp
            .path()
            .join("user.slice/user@1001.service/app.slice/pw.service");
        fs::create_dir_all(unit.join(COORDINATOR)).unwrap();
        // Where this process is once it left: what `finish` reads back before the kill.
        fs::write(
            unit.with_file_name("self-cgroup"),
            "0::/user.slice/user@1001.service/app.slice/pw.service/coordinator\n",
        )
        .unwrap();
        (temp, unit)
    }

    fn fake_config(unit: &Path) -> Config {
        Config {
            interval: Duration::from_millis(20),
            kill_wait: Duration::from_millis(60),
            require_cgroup2: false,
            self_cgroup: unit.with_file_name("self-cgroup"),
        }
    }

    #[test]
    fn an_attempt_reads_its_counters_kills_leftovers_and_removes_itself() {
        let (_temp, unit) = fake_unit();
        let attempt = enter_attempt_with(&unit, "a1b2", 1000, fake_config(&unit)).unwrap();
        let directory = attempt.path().to_path_buf();
        assert_eq!(directory, unit.join("attempt-a1b2"));
        let me = std::process::id();
        assert_eq!(
            fs::read_to_string(directory.join("cgroup.procs")).unwrap(),
            me.to_string()
        );
        let mib = 1u64 << 20;
        fs::write(
            directory.join("memory.stat"),
            format!("anon {}\nkernel 0\n", 950 * mib),
        )
        .unwrap();
        thread::sleep(Duration::from_millis(80));
        attempt.command_started();
        fs::write(directory.join("memory.peak"), "3000000000\n").unwrap();
        fs::write(directory.join("memory.events"), EVENTS).unwrap();
        fs::write(directory.join("cpu.stat"), CPU).unwrap();
        fs::write(directory.join("pids.peak"), "17\n").unwrap();
        fs::write(directory.join("memory.pressure"), PRESSURE).unwrap();
        fs::write(directory.join("cpu.pressure"), PRESSURE).unwrap();
        // Twelve leftovers that exist nowhere (beyond pid_max), and this process.
        let ghosts: Vec<u32> = (0..12).map(|i| 999_999_000 + i).collect();
        let procs: Vec<String> = std::iter::once(me)
            .chain(ghosts.iter().copied())
            .map(|p| p.to_string())
            .collect();
        fs::write(directory.join("cgroup.procs"), procs.join("\n")).unwrap();
        let kill = directory.join("cgroup.kill");
        // Observe the kill before the fake tree is removed.
        let watched = {
            let kill = kill.clone();
            thread::spawn(move || {
                let started = Instant::now();
                while started.elapsed() < Duration::from_secs(5) {
                    if let Ok(text) = fs::read_to_string(&kill) {
                        return Some(text);
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                None
            })
        };
        let evidence = attempt.finish();
        assert_eq!(
            watched.join().unwrap().as_deref(),
            Some("1"),
            "leftovers killed"
        );
        assert_eq!(evidence["memory_peak_bytes"], 3_000_000_000u64);
        assert_eq!(evidence["swap_peak_bytes"], Value::Null);
        assert_eq!(evidence["noncache_peak_bytes"], 950 * mib);
        assert_eq!(evidence["noncache_formula"], NONCACHE_KERNEL);
        // Peaked from the first sample, before the command started.
        assert!(evidence["noncache_peak_at_seconds"].as_f64().unwrap() < 0.0);
        assert!(evidence["seconds_above_90pct"].as_f64().unwrap() > 0.0);
        assert_eq!(
            (evidence["oom"].clone(), evidence["oom_kill"].clone()),
            (json!(2), json!(1))
        );
        assert_eq!(evidence["cpu_usec"], 7_654_321);
        assert_eq!(evidence["pids_peak"], 17);
        assert_eq!(evidence["memory_pressure_full_usec"], 7890);
        assert_eq!(evidence["cpu_pressure_some_usec"], 123_456);
        assert_eq!(evidence["leftover_pids"]["count"], 12);
        assert_eq!(evidence["leftover_pids"]["pids"], json!(ghosts[..10]));
        // A fake tree's procs never empty.
        assert_eq!(evidence["leftover_after_kill"], 12);
        assert_eq!(
            fs::read_to_string(unit.join("coordinator/cgroup.procs")).unwrap(),
            me.to_string(),
            "left the attempt cgroup before the kill"
        );
        assert!(!directory.exists(), "removed");
        assert_eq!(evidence.get("errors"), None, "{evidence}");
    }

    #[test]
    fn an_attempt_alone_kills_nothing() {
        let (_temp, unit) = fake_unit();
        let attempt = enter_attempt_with(&unit, "solo", 0, fake_config(&unit)).unwrap();
        let directory = attempt.path().to_path_buf();
        let evidence = attempt.finish();
        assert_eq!(evidence.get("leftover_pids"), None);
        assert_eq!(
            evidence["seconds_above_90pct"],
            Value::Null,
            "no reservation"
        );
        assert_eq!(
            evidence["noncache_peak_bytes"],
            Value::Null,
            "no memory.stat"
        );
        assert_eq!(evidence["samples"], 0);
        assert!(!directory.exists());
    }

    /// `cgroup.kill` ends every process in the attempt's cgroup: written while the supervisor is
    /// still there, it would end the supervisor before its result. Not out, the leftovers are
    /// killed one by one and the supervisor returns.
    #[test]
    fn a_supervisor_still_inside_never_writes_cgroup_kill() {
        for case in ["no coordinator leaf", "moved but still inside"] {
            let (_temp, unit) = fake_unit();
            let config = fake_config(&unit);
            if case == "no coordinator leaf" {
                fs::remove_dir_all(unit.join(COORDINATOR)).unwrap();
            } else {
                fs::write(
                    &config.self_cgroup,
                    "0::/user.slice/user@1001.service/app.slice/pw.service/attempt-inside\n",
                )
                .unwrap();
            }
            let attempt = enter_attempt_with(&unit, "inside", 0, config).unwrap();
            let directory = attempt.path().to_path_buf();
            let me = std::process::id();
            fs::write(directory.join("cgroup.procs"), format!("{me}\n999999001\n")).unwrap();
            // Any write of cgroup.kill fails and says so in `errors`.
            fs::create_dir(directory.join("cgroup.kill")).unwrap();
            let evidence = attempt.finish();
            assert_eq!(evidence["leftover_pids"]["count"], 1, "{case}: {evidence}");
            let errors = evidence["errors"].to_string();
            assert!(!errors.contains("cgroup.kill"), "{case}: {errors}");
            if case == "no coordinator leaf" {
                assert!(errors.contains("leave for"), "{case}: {errors}");
            }
        }
    }

    #[test]
    fn entering_fails_without_side_effects() {
        let (_temp, unit) = fake_unit();
        assert!(enter_attempt_with(&unit, "../x", 0, fake_config(&unit)).is_err());
        assert!(enter_attempt_with(&unit, "", 0, fake_config(&unit)).is_err());
        assert!(enter_attempt_with(Path::new("relative"), "a", 0, fake_config(&unit)).is_err());
        let missing = unit.join("absent");
        let error = enter_attempt_with(&missing, "a", 0, fake_config(&unit))
            .err()
            .unwrap();
        assert!(error.contains("create"), "{error}");
        // Production refuses anything that is not cgroup2, such as a temporary directory.
        let error = enter_attempt(&unit, "a", 0).err().unwrap();
        assert!(error.contains("not on a cgroup2"), "{error}");
        assert!(!unit.join("attempt-a").exists());
        // The same attempt twice: an earlier cgroup's peak is never taken for this one's.
        let first = enter_attempt_with(&unit, "twice", 0, fake_config(&unit)).unwrap();
        assert!(enter_attempt_with(&unit, "twice", 0, fake_config(&unit)).is_err());
        first.finish();
    }

    /// On a real delegated cgroup, as the executor's unit runs (the user unit's drop-in), and
    /// alone in it. Changes no unit: a transient user service that ends with the test. Started
    /// in `<unit>/coordinator`, it enables the controllers as the coordinator does
    /// (`patchweft`'s `cgroup::Host::prepare_coordinator`, tested on its own).
    ///
    /// ```text
    /// systemd-run --user --wait --pipe --quiet -p 'Delegate=memory pids' \
    ///   -p DelegateSubgroup=coordinator -- runner/target/debug/deps/patchweft_runner-<hash> \
    ///   --ignored --exact cgroup::tests::on_a_real_delegated_cgroup
    /// ```
    #[test]
    #[ignore = "needs a delegated cgroup: run inside a transient user service (see the doc)"]
    fn on_a_real_delegated_cgroup() {
        let own = || {
            parse_self_cgroup(&fs::read_to_string("/proc/self/cgroup").unwrap())
                .unwrap()
                .to_owned()
        };
        assert!(own().ends_with("/coordinator"), "not delegated: {}", own());
        let unit = Path::new("/sys/fs/cgroup")
            .join(own().trim_start_matches('/'))
            .parent()
            .unwrap()
            .to_path_buf();
        // Never `+cpu`: that would weigh attempts' CPU against each other, and measuring only is the
        // promise. `cpu.stat` and `cpu.pressure` exist on every cgroup without it.
        fs::write(unit.join("cgroup.subtree_control"), "+memory +pids").unwrap();
        // Why the coordinator needs its leaf: no process may join the unit's own cgroup now.
        let refused = fs::write(unit.join("cgroup.procs"), std::process::id().to_string());
        assert_eq!(refused.unwrap_err().raw_os_error(), Some(libc::EBUSY));
        let config = Config {
            interval: Duration::from_millis(200),
            ..Config::default()
        };
        let attempt = enter_attempt_with(&unit, "itest", 64, config).unwrap();
        assert!(own().ends_with("/attempt-itest"), "{}", own());
        let mut hog = std::process::Command::new("/usr/bin/python3")
            .args(["-c", "import time; b = b'x' * (200 << 20); time.sleep(2)"])
            .spawn()
            .unwrap();
        attempt.command_started();
        // A process the job left behind; `finish` kills and reaps it.
        #[allow(clippy::zombie_processes)]
        let leftover = std::process::Command::new("/usr/bin/sleep")
            .arg("300")
            .spawn()
            .unwrap();
        assert!(hog.wait().unwrap().success());
        let evidence = attempt.finish();
        eprintln!("{evidence:#}");
        let mib = 1u64 << 20;
        assert!(evidence["memory_peak_bytes"].as_u64().unwrap() >= 200 * mib);
        assert!(evidence["noncache_peak_bytes"].as_u64().unwrap() >= 200 * mib);
        assert!(evidence["noncache_peak_at_seconds"].as_f64().unwrap() >= 0.0);
        assert!(evidence["seconds_above_90pct"].as_f64().unwrap() > 0.0);
        assert!(evidence["cpu_usec"].as_u64().unwrap() > 0);
        assert!(evidence["cpu_pressure_some_usec"].is_u64(), "{evidence}");
        let controllers = fs::read_to_string(unit.join("cgroup.subtree_control")).unwrap();
        assert!(
            !controllers.split_whitespace().any(|c| c == "cpu"),
            "{controllers}"
        );
        assert!(evidence["pids_peak"].as_u64().unwrap() >= 3);
        assert_eq!(evidence["oom_kill"], 0);
        assert_eq!(
            evidence["leftover_pids"],
            json!({"count":1,"pids":[leftover.id()]})
        );
        assert_eq!(evidence.get("leftover_after_kill"), None);
        assert_eq!(evidence.get("errors"), None);
        assert!(!unit.join("attempt-itest").exists(), "removed");
        assert!(own().ends_with("/coordinator"), "{}", own());
        assert!(
            !Path::new(&format!("/proc/{}", leftover.id())).exists(),
            "the leftover was killed and reaped"
        );
    }
    /// A unit whose `coordinator` leaf is missing, on a real delegated cgroup: leaving fails, so
    /// `finish` must kill the job's leftover by pid and return, never write `cgroup.kill` on the
    /// cgroup it is still in (which would end this test's process). Run as
    /// `on_a_real_delegated_cgroup` is, with `--exact cgroup::tests::a_real_unit_without_its_leaf`.
    #[test]
    #[ignore = "needs a delegated cgroup: run inside a transient user service (see the doc)"]
    fn a_real_unit_without_its_leaf() {
        let own = || {
            parse_self_cgroup(&fs::read_to_string("/proc/self/cgroup").unwrap())
                .unwrap()
                .to_owned()
        };
        assert!(own().ends_with("/coordinator"), "not delegated: {}", own());
        let leaf = Path::new("/sys/fs/cgroup").join(own().trim_start_matches('/'));
        // A unit of its own under the delegated one, with no coordinator leaf.
        let unit = leaf.parent().unwrap().join("leafless");
        fs::create_dir(&unit).unwrap();
        let config = Config {
            interval: Duration::from_millis(200),
            ..Config::default()
        };
        let attempt = enter_attempt_with(&unit, "p21", 0, config).unwrap();
        assert!(own().ends_with("/leafless/attempt-p21"), "{}", own());
        #[allow(clippy::zombie_processes)]
        let leftover = std::process::Command::new("/usr/bin/sleep")
            .arg("300")
            .spawn()
            .unwrap();
        let evidence = attempt.finish();
        eprintln!("{evidence:#}");
        // Still here to assert: the supervisor was not killed with its attempt's cgroup.
        assert_eq!(
            evidence["leftover_pids"],
            json!({"count":1,"pids":[leftover.id()]})
        );
        assert!(
            evidence["errors"].to_string().contains("leave for"),
            "{evidence}"
        );
        assert!(
            !Path::new(&format!("/proc/{}", leftover.id())).exists(),
            "the leftover was killed by pid and reaped"
        );
        fs::write(leaf.join("cgroup.procs"), std::process::id().to_string()).unwrap();
        fs::remove_dir(unit.join("attempt-p21")).unwrap();
        fs::remove_dir(&unit).unwrap();
    }
}
