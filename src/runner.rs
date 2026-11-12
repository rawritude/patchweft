//! The runner, `patchweft-runner`: the separate binary that executes every job (`runner/`). This
//! binary schedules, serves and coordinates, and never runs a job itself.
//!
//! A profile's `engine` is the runner's sha256, so only a runner release changes profile ids and
//! recipes. The runner is installed beside this binary, under a directory named by its own hash:
//!
//! ```text
//! <directory of patchweft>/patchweft
//! <directory of patchweft>/runner/<sha256>/patchweft-runner
//! ```
//!
//! `runner/` holds exactly that one hash directory. `--runner PATH` names another file instead.
//! A coordinator hashes the runner when it starts, and again just before each launch, from the
//! bytes it then executes, sealed in memory.
use anyhow::{Context, Result, bail, ensure};
use patchweft_control::{file_hash, reader_hash};
use std::{
    ffi::{CStr, CString, OsStr},
    fs::{self, File},
    io,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::{ffi::OsStrExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::OnceLock,
};

pub const BINARY: &str = "patchweft-runner";
/// Beside this binary, the directory that holds the runner's hash directory.
pub const DIRECTORY: &str = "runner";

static OVERRIDE: OnceLock<PathBuf> = OnceLock::new();

/// `--runner PATH`: this process uses that runner instead of the installed one. Set once.
pub fn set_override(path: &Path) -> Result<()> {
    let path = path
        .canonicalize()
        .with_context(|| format!("runner {}", path.display()))?;
    ensure!(
        OVERRIDE.get_or_init(|| path.clone()) == &path,
        "another runner was already named"
    );
    Ok(())
}

/// The runner this process launches or registers profiles for: `--runner`, else the one
/// installed beside this binary.
pub fn path() -> Result<PathBuf> {
    if let Some(path) = OVERRIDE.get() {
        return Ok(path.clone());
    }
    // A unit-test harness has no runner beside it; its own bytes stand in, as this binary's did
    // before the split. Nothing in the library's unit tests launches a runner.
    if cfg!(test) {
        return Ok(std::env::current_exe()?);
    }
    installed(&std::env::current_exe()?)
}

/// `<directory of exe>/runner/<sha256>/patchweft-runner`, the one hash directory there.
pub fn installed(exe: &Path) -> Result<PathBuf> {
    let directory = exe
        .parent()
        .context("this binary has no directory")?
        .join(DIRECTORY);
    let mut names = fs::read_dir(&directory)
        .with_context(|| format!("no runner installed in {}", directory.display()))?
        .map(|entry| Ok(entry?.file_name()))
        .collect::<io::Result<Vec<_>>>()?;
    names.sort();
    match names.as_slice() {
        [name] => {
            let name = name
                .to_str()
                .filter(|name| is_sha256(name))
                .with_context(|| {
                    format!(
                        "{} must hold one directory named by the runner's sha256",
                        directory.display()
                    )
                })?;
            Ok(directory.join(name).join(BINARY))
        }
        [] => bail!("no runner installed in {}", directory.display()),
        _ => bail!(
            "{} holds {} entries; install exactly one runner, as {DIRECTORY}/<sha256>/{BINARY}",
            directory.display(),
            names.len()
        ),
    }
}

fn is_sha256(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The hash a runner's directory names, when it is installed as `<sha256>/patchweft-runner`.
fn named_hash(path: &Path) -> Option<&str> {
    (path.file_name()? == BINARY)
        .then(|| path.parent()?.file_name()?.to_str())
        .flatten()
        .filter(|name| is_sha256(name))
}

/// A runner and the sha256 of its bytes when it was located.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Runner {
    pub path: PathBuf,
    pub sha256: String,
}

impl Runner {
    /// The runner of [`path`], hashed.
    pub fn locate() -> Result<Self> {
        Self::at(&path()?)
    }

    /// The runner at `path`, hashed. One installed under a hash directory must have that hash.
    pub fn at(path: &Path) -> Result<Self> {
        let sha256 = file_hash(path).with_context(|| format!("runner {}", path.display()))?;
        if let Some(named) = named_hash(path) {
            ensure!(
                named == sha256,
                "runner {} has sha256 {sha256}, not the one its directory names",
                path.display()
            );
        }
        Ok(Self {
            path: path.to_owned(),
            sha256,
        })
    }

    /// Copies the runner's bytes into a sealed memfd and hashes that, refusing it unless its
    /// bytes are `expected` (the claimed profile's engine). [`Opened::spawn`] executes that same
    /// memfd, whose seals forbid any change, so what runs is what was hashed, even if the file is
    /// replaced or rewritten in place meanwhile.
    pub fn open(&self, expected: &str) -> Result<Opened> {
        let mut source =
            File::open(&self.path).with_context(|| format!("runner {}", self.path.display()))?;
        let file = sealed_copy(&mut source)
            .with_context(|| format!("runner {}: sealed copy", self.path.display()))?;
        drop(source);
        let sha256 = reader_hash(&file)?;
        ensure!(
            sha256 == expected,
            "runner {} has sha256 {sha256}, not the profile's engine {expected}; \
             refusing to launch it",
            self.path.display()
        );
        Ok(Opened {
            file,
            path: self.path.clone(),
        })
    }
}

/// `memfd_create(2)`, or a test's stand-in for it.
type CreateMemfd = fn(&CStr, libc::c_uint) -> io::Result<RawFd>;

fn create_memfd(name: &CStr, flags: libc::c_uint) -> io::Result<RawFd> {
    // SAFETY: a valid C string.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), flags) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(fd)
    }
}

/// A new memfd that may be executed, sealable and close-on-exec. `MFD_EXEC` asks for execution
/// explicitly, which a host with `vm.memfd_noexec=1` needs: there a memfd made without it is
/// sealed non-executable and `fexecve` fails with EACCES. Kernels before 6.3 know no `MFD_EXEC`
/// and refuse it with EINVAL; their memfds are always executable, so on EINVAL alone it is made
/// again without it. (`vm.memfd_noexec=2` refuses `MFD_EXEC` with EACCES: no runner can launch.)
fn executable_memfd(name: &CStr, create: CreateMemfd) -> io::Result<RawFd> {
    let flags = libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING;
    match create(name, flags | libc::MFD_EXEC) {
        Err(error) if error.raw_os_error() == Some(libc::EINVAL) => create(name, flags),
        made => made,
    }
}

/// `source`'s bytes in a new memfd, sealed against any write, growth or shrinking (and against
/// new seals), read-only and positioned at its start. Close-on-exec, which `fexecve` of an ELF
/// file allows.
fn sealed_copy(source: &mut File) -> io::Result<File> {
    sealed_copy_with(source, create_memfd)
}

fn sealed_copy_with(source: &mut File, create: CreateMemfd) -> io::Result<File> {
    let name = CString::new(BINARY).expect("static name");
    let fd = executable_memfd(&name, create)?;
    // SAFETY: a new descriptor, owned by the File made from it.
    let mut memfd = unsafe { File::from_raw_fd(fd) };
    io::copy(source, &mut memfd)?;
    let seals = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    if unsafe { libc::fcntl(memfd.as_raw_fd(), libc::F_ADD_SEALS, seals) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // Executing needs no descriptor open for writing (ETXTBSY): a read-only one in its place.
    let readonly = File::open(format!("/proc/self/fd/{}", memfd.as_raw_fd()))?;
    drop(memfd);
    Ok(readonly)
}

/// A runner's bytes, sealed in memory and checked by [`Runner::open`].
#[derive(Debug)]
pub struct Opened {
    file: File,
    path: PathBuf,
}

impl Opened {
    /// Launches `patchweft-runner ARGS…` from the sealed bytes, through `fexecve`: only `PATH` in
    /// its environment, stdin from /dev/null, stdout and stderr to `log`, in its own process
    /// group. Its argv[0] is the runner's path, so operators find it by that path in `ps`; its
    /// `/proc/self/exe` names the memfd, `/memfd:patchweft-runner (deleted)`, whose content the
    /// runner hashes again.
    pub fn spawn(self, args: &[&OsStr], log: File) -> Result<Child> {
        let mut strings = vec![CString::new(self.path.as_os_str().as_bytes())?];
        for arg in args {
            strings.push(CString::new(arg.as_bytes())?);
        }
        let environment = vec![CString::new("PATH=/usr/bin:/bin")?];
        let exec = Exec::new(self.file.as_raw_fd(), strings, environment);
        let mut command = Command::new(&self.path);
        command
            .args(args)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0);
        // SAFETY: the closure only calls fexecve with pointers built before the fork; it
        // allocates nothing. The descriptor is close-on-exec, which fexecve of an ELF file allows.
        unsafe {
            command.pre_exec(move || exec.run());
        }
        let child = command.spawn()?;
        drop(self.file);
        Ok(child)
    }
}

/// `fexecve`'s arguments, built before the fork.
struct Exec {
    fd: RawFd,
    _strings: Vec<CString>,
    argv: Vec<*const libc::c_char>,
    envp: Vec<*const libc::c_char>,
}

// SAFETY: the pointers point into the strings `Exec` owns, which nothing mutates or frees first.
unsafe impl Send for Exec {}
unsafe impl Sync for Exec {}

impl Exec {
    fn new(fd: RawFd, strings: Vec<CString>, environment: Vec<CString>) -> Self {
        let pointers = |strings: &[CString]| {
            strings
                .iter()
                .map(|s| s.as_ptr())
                .chain([std::ptr::null()])
                .collect()
        };
        let argv = pointers(&strings);
        let envp = pointers(&environment);
        let mut owned = strings;
        owned.extend(environment);
        Self {
            fd,
            _strings: owned,
            argv,
            envp,
        }
    }

    fn run(&self) -> io::Result<()> {
        // SAFETY: argv and envp are null-terminated arrays of valid C strings.
        unsafe {
            libc::fexecve(self.fd, self.argv.as_ptr(), self.envp.as_ptr());
        }
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// A copy of `/usr/bin/true` or `/usr/bin/false` at `path`.
    fn program(source: &str, path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::copy(source, path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn the_installed_runner_is_the_one_hash_directory_beside_the_binary() {
        let temp = tempfile::tempdir().unwrap();
        let exe = temp.path().join("patchweft");
        let refusal = |exe: &Path| format!("{:#}", installed(exe).unwrap_err());
        assert!(refusal(&exe).contains("no runner installed"));
        let staging = temp.path().join("staging");
        program("/usr/bin/true", &staging);
        let sha256 = file_hash(&staging).unwrap();
        let path = temp.path().join(DIRECTORY).join(&sha256).join(BINARY);
        program("/usr/bin/true", &path);
        assert_eq!(installed(&exe).unwrap(), path);
        let runner = Runner::at(&path).unwrap();
        assert_eq!(runner.sha256, sha256);

        // Two runners beside one binary: which one runs would be a guess.
        let other = temp
            .path()
            .join(DIRECTORY)
            .join("f".repeat(64))
            .join(BINARY);
        program("/usr/bin/false", &other);
        assert!(
            refusal(&exe).contains("holds 2 entries"),
            "{}",
            refusal(&exe)
        );
        // A runner whose bytes are not the hash its directory names is refused.
        let error = format!("{:#}", Runner::at(&other).unwrap_err());
        assert!(error.contains("not the one its directory names"), "{error}");
        fs::remove_dir_all(other.parent().unwrap()).unwrap();
        // One entry that is not a hash names no runner.
        fs::rename(
            path.parent().unwrap(),
            temp.path().join(DIRECTORY).join("latest"),
        )
        .unwrap();
        assert!(refusal(&exe).contains("named by the runner's sha256"));
    }

    #[test]
    fn spawn_executes_the_hashed_bytes_even_when_the_file_is_rewritten_in_place() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(BINARY);
        program("/usr/bin/true", &path);
        let runner = Runner::at(&path).unwrap();
        let opened = runner.open(&runner.sha256).unwrap();
        // The same inode, other bytes: an open file descriptor would now read `false`.
        fs::write(&path, fs::read("/usr/bin/false").unwrap()).unwrap();
        assert_ne!(file_hash(&path).unwrap(), runner.sha256);
        let log = File::create(temp.path().join("log")).unwrap();
        let mut child = opened.spawn(&[], log).unwrap();
        assert!(child.wait().unwrap().success(), "true ran, not false");
    }

    thread_local! {
        static FLAGS: std::cell::RefCell<Vec<libc::c_uint>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    /// A kernel before 6.3: `MFD_EXEC` is an unknown flag.
    fn before_6_3(name: &CStr, flags: libc::c_uint) -> io::Result<RawFd> {
        FLAGS.with(|seen| seen.borrow_mut().push(flags));
        if flags & libc::MFD_EXEC != 0 {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        create_memfd(name, flags)
    }

    fn refusing(_: &CStr, flags: libc::c_uint) -> io::Result<RawFd> {
        FLAGS.with(|seen| seen.borrow_mut().push(flags));
        Err(io::Error::from_raw_os_error(libc::EACCES))
    }

    #[test]
    fn the_memfd_asks_for_exec_and_without_it_only_where_the_kernel_knows_no_such_flag() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(BINARY);
        program("/usr/bin/true", &path);
        let base = libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING;
        // EINVAL: asked again without MFD_EXEC, and what it makes runs.
        let copy = sealed_copy_with(&mut File::open(&path).unwrap(), before_6_3).unwrap();
        assert_eq!(
            FLAGS.with(|seen| seen.take()),
            [base | libc::MFD_EXEC, base]
        );
        assert_eq!(reader_hash(&copy).unwrap(), file_hash(&path).unwrap());
        let opened = Opened {
            file: copy,
            path: path.clone(),
        };
        let log = File::create(temp.path().join("log")).unwrap();
        assert!(opened.spawn(&[], log).unwrap().wait().unwrap().success());
        // Any other refusal is the error: never retried without the flag.
        let error = sealed_copy_with(&mut File::open(&path).unwrap(), refusing).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EACCES));
        assert_eq!(FLAGS.with(|seen| seen.take()), [base | libc::MFD_EXEC]);
    }

    /// Each materialization launches from its own thread: a fork while another thread holds its
    /// writable memfd copies that descriptor into the child, which must never make an exec fail
    /// (ETXTBSY).
    #[test]
    fn concurrent_launches_from_many_threads_all_run() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(BINARY);
        program("/usr/bin/true", &path);
        let runner = Runner::at(&path).unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let runner = runner.clone();
                std::thread::spawn(move || {
                    let mut failures = Vec::new();
                    for _ in 0..25 {
                        let log = tempfile::tempfile().unwrap();
                        match runner
                            .open(&runner.sha256)
                            .and_then(|opened| opened.spawn(&[], log))
                        {
                            Ok(mut child) => assert!(child.wait().unwrap().success()),
                            Err(error) => failures.push(format!("{error:#}")),
                        }
                    }
                    failures
                })
            })
            .collect();
        let failures: Vec<String> = threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect();
        assert!(
            failures.is_empty(),
            "{} failed: {:?}",
            failures.len(),
            failures.first()
        );
    }

    #[test]
    fn spawn_refuses_a_runner_whose_hash_differs() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(BINARY);
        program("/usr/bin/true", &path);
        let runner = Runner::at(&path).unwrap();
        let error = format!("{:#}", runner.open(&"0".repeat(64)).unwrap_err());
        assert!(
            error.contains("not the profile's engine") && error.contains("refusing to launch"),
            "{error}"
        );
        // The same path with other bytes than when it was located is refused too.
        let replacement = temp.path().join("replacement");
        program("/usr/bin/false", &replacement);
        fs::rename(&replacement, &path).unwrap();
        assert!(runner.open(&runner.sha256).is_err());
    }

    #[test]
    fn spawn_executes_the_bytes_it_hashed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(BINARY);
        program("/usr/bin/true", &path);
        let runner = Runner::at(&path).unwrap();
        let opened = runner.open(&runner.sha256).unwrap();
        // Replaced between the check and the launch: the checked bytes still run.
        let replacement = temp.path().join("replacement");
        program("/usr/bin/false", &replacement);
        fs::rename(&replacement, &path).unwrap();
        let log = File::create(temp.path().join("log")).unwrap();
        let mut child = opened.spawn(&[OsStr::new("_supervise")], log).unwrap();
        assert!(child.wait().unwrap().success(), "true ran, not false");
        // And the replacement itself is what a fresh check sees.
        let log = File::create(temp.path().join("log")).unwrap();
        let replaced = Runner::at(&path).unwrap();
        let mut child = replaced
            .open(&replaced.sha256)
            .unwrap()
            .spawn(&[], log)
            .unwrap();
        assert!(!child.wait().unwrap().success());
    }
}
