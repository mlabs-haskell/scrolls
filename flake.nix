{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    pre-commit-hooks-nix = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.nixpkgs-stable.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };


  outputs =
    inputs @ { self
    , ...
    }: inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      systems =
        if builtins.hasAttr "currentSystem" builtins
        then [ builtins.currentSystem ]
        else inputs.nixpkgs.lib.systems.flakeExposed;
      flake = {
        herculesCI.ciSystems = [ "x86_64-linux" ];

        nixosModules.scrolls = { pkgs, lib, ... }: {
          imports = [ ./scrolls-nixos-module.nix ];
          services.scrolls.package = lib.mkOptionDefault self.packages.${pkgs.system}.scrolls;
        };
      };
      perSystem =
        { config
        , self'
        , inputs'
        , pkgs
        , system
        , ...
        }: {
          packages = {
            scrolls = inputs.crane.lib.${system}.buildPackage {
              src = self;
            };
            default = self'.packages.scrolls;
          };

          devShells.default = pkgs.mkShell {
            nativeBuildInputs = [
              pkgs.cargo
              pkgs.rustc
              pkgs.cargo-outdated
              pkgs.rustfmt
              pkgs.rust-analyzer
              pkgs.docker-compose
              pkgs.redis
            ];
          };

          formatter = pkgs.nixpkgs-fmt;
        };
    };
}
