use std::any::Any;

pub trait AppHost: Any + Send + Sync {}

impl<T> AppHost for T where T: Any + Send + Sync {}
