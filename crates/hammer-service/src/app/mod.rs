use std::any::Any;

use hammer_core::Lifecycle;

pub trait AppHost: Lifecycle + Any + Send + Sync {}

impl<T> AppHost for T where T: Lifecycle + Any + Send + Sync {}
