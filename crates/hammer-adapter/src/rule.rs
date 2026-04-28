// Rule / RuleSet surface — kept as marker traits in M2 because the actual
// matching logic lands with the rule engine in M4. Concrete `RuleAction`
// variants already exist in hammer_core::config::options::RuleActionKind.

pub trait HeadlessRule: Send + Sync + 'static {}

pub trait Rule: HeadlessRule {
    fn type_name(&self) -> &str;
}
