use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=ROBOTUNNEL_BUILD_DATE");
    println!("cargo:rerun-if-env-changed=ROBOTUNNEL_GIT_COMMIT");

    let build_date =
        env::var("ROBOTUNNEL_BUILD_DATE").unwrap_or_else(|_| String::from("unknown"));
    let git_commit =
        env::var("ROBOTUNNEL_GIT_COMMIT").unwrap_or_else(|_| String::from("unknown"));

    println!("cargo:rustc-env=ROBOTUNNEL_BUILD_DATE={build_date}");
    println!("cargo:rustc-env=ROBOTUNNEL_GIT_COMMIT={git_commit}");
}
