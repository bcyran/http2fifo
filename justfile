# Run all QA checks (formatting + lints + tests). Does not modify any files.
check: fmt-check lint test

# Check formatting without modifying files.
fmt-check:
    cargo fmt --all -- --check

# Fix formatting in-place.
fmt:
    cargo fmt --all

# Check lints without modifying files.
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Auto-fix lint warnings where possible.
lint-fix:
    cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged -- -D warnings

# Run the test suite.
test:
    cargo test --all-targets

# Fix both formatting and auto-fixable lints in one shot.
fix: fmt lint-fix
