//! The public `Result` alias and `GlommioError` both default their payload
//! type parameter to `()`, so the common case can be named with one parameter
//! and the explicit form keeps compiling. These are compile-time guarantees,
//! so most of this file is the signatures rather than the assertions.

use glommio::{GlommioError, Result};
use std::time::Duration;

fn one_parameter() -> Result<u32> {
    Ok(7)
}

fn two_parameters() -> Result<u32, ()> {
    Ok(9)
}

fn carries_a_payload() -> Result<(), String> {
    Err(GlommioError::Closed(glommio::ResourceType::Channel(
        String::from("unsent"),
    )))
}

#[test]
fn alias_accepts_one_or_two_parameters() {
    assert_eq!(one_parameter().unwrap(), 7);
    assert_eq!(two_parameters().unwrap(), 9);
}

#[test]
fn error_can_be_named_without_a_parameter() {
    let err: GlommioError = GlommioError::TimedOut(Duration::from_secs(1));
    assert!(err.to_string().starts_with("Operation timed out after"));
}

#[test]
fn defaulting_does_not_disturb_a_real_payload() {
    let err = carries_a_payload().unwrap_err();
    assert_eq!(err.into_inner().as_deref(), Some("unsent"));
}
