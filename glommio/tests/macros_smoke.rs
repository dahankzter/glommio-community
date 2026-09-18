// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
//! `#[glommio::test]` from the position a user writes it in.

use glommio::CpuSet;

#[glommio::test]
async fn runs_an_async_body() {
    let value = glommio::spawn_local(async { 21u32 * 2 }).await;
    assert_eq!(value, 42, "the body ran on an executor");
}

#[glommio::test(placement = Fixed(0))]
async fn honours_a_placement() {
    glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
}

#[glommio::test]
async fn returns_a_result() -> Result<(), std::io::Error> {
    Ok(())
}

/// The tokens after `placement =` are emitted with `::glommio::Placement::`
/// prepended, so the variant is fixed at the attribute and its argument is
/// not. That is what makes choosing exact cores work without the attribute
/// having to know anything about `CpuSet`.
#[glommio::test(placement = Fenced(CpuSet::online().unwrap().filter(|l| l.cpu < 2)))]
async fn fenced_to_chosen_cores() {
    glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
}

/// Selection is on a `CpuLocation`, so NUMA node and package work the same way
/// as the cpu index does.
#[glommio::test(placement = Fenced(CpuSet::online().unwrap().filter(|l| l.numa_node == 0)))]
async fn fenced_to_a_numa_node() {
    glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
}

/// The expansion emits a plain `#[test]`, so the harness attributes compose
/// without this macro knowing anything about them.
#[glommio::test]
#[should_panic(expected = "deliberate")]
async fn should_panic_composes() {
    glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
    panic!("deliberate");
}

#[glommio::test]
#[ignore = "composes with ignore"]
async fn ignore_composes() {
    unreachable!("ignored tests do not run");
}

/// And in the other order, since attribute order is a fair thing to get wrong.
#[should_panic(expected = "deliberate")]
#[glommio::test]
async fn should_panic_composes_either_order() {
    panic!("deliberate");
}
