#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Driver,
    PreInput,
    Internal,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NodeState {
    Disabled,
    #[default]
    Polling,
    Interrupt,
}
