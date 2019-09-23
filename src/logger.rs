use log::log;
pub use log::Level;

pub trait Logger {
    fn log(&self, level: Level, file: &str, line: i32, message: &str);
}

pub struct DefaultLogger;

impl Logger for DefaultLogger {
    fn log(&self, level: Level, file: &str, line: i32, message: &str) {
        log!(level, "{}:{} - {}", file, line, message);
    }
}
