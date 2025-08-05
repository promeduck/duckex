# SPDX-FileCopyrightText: 2025 Stas Muzhyk <sts@abc3.dev>
# SPDX-FileCopyrightText: 2025 Łukasz Niemier <~@hauleth.dev>
#
# SPDX-License-Identifier: Apache-2.0

{
  description = "Elixir's application";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  inputs.flake-parts.url = "github:hercules-ci/flake-parts";

  inputs.devenv = {
    url = "github:cachix/devenv";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    flake-parts,
    devenv,
    ...
  } @ inputs:
    flake-parts.lib.mkFlake {inherit inputs;} {
      flake = {};

      systems = [
        "x86_64-linux"
        "x86_64-darwin"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      perSystem = {
        self',
        inputs',
        pkgs,
        lib,
        ...
      }: {
        formatter = pkgs.alejandra;

        packages = {
          # Expose Devenv supervisor
          devenv-up = self'.devShells.default.config.procfileScript;
        };

        devShells.default = devenv.lib.mkShell {
          inherit inputs pkgs;

          modules = let
              otp = pkgs.beam.packages.erlang_27;
            in [
              {
                languages.elixir = {
                  enable = true;
                  package = otp.elixir_1_18;
                };
                languages.erlang = {
                  enable = true;
                  package = otp.erlang;
                };

                # env.DYLD_INSERT_LIBRARIES = "${pkgs.mimalloc}/lib/libmimalloc.dylib";
              }
              {
                languages.rust.enable = true;
                packages = [
                  pkgs.duckdb
                  pkgs.cargo-outdated
                ];
              }
            ];
          };
        };
    };
}
