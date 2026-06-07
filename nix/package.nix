{
  lib,
  rustPlatform,
  rev ? "dirty",
}: let
  cargoToml = builtins.fromTOML (builtins.readFile ../Cargo.toml);
in
  rustPlatform.buildRustPackage {
    pname = "http2fifo";
    version = "${cargoToml.package.version}-${rev}";

    src = lib.fileset.toSource {
      root = ../.;
      fileset = lib.fileset.intersection (lib.fileset.fromSource (lib.sources.cleanSource ../.)) (
        lib.fileset.unions [
          ../src
          ../Cargo.toml
          ../Cargo.lock
        ]
      );
    };

    cargoLock.lockFile = ../Cargo.lock;

    meta = {
      description = "Mount an HTTP streaming resource as a Unix named pipe (FIFO)";
      mainProgram = "http2fifo";
    };
  }
