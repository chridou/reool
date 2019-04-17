mod error;
pub(crate) mod executor_flavour;
mod pool;

pub use crate::pool::Config;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
