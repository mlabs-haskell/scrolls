{ config, lib, pkgs, ... }:
let
  cfg = config.services.scrolls;
  scrollsName = n: "scrolls-${n}";
  enabledInstnaces = lib.filterAttrs (name: conf: conf.enable) cfg.instances;
in
with lib;
{
  options.services.scrolls = with types; {
    package = mkOption {
      description = "Scrolls package.";
      type = package;
    };

    instances = mkOption {
      type = attrsOf (submodule ({ config, name, ... }@args: {
        options = {
          enable = mkEnableOption (scrollsName name);

          user = mkOption {
            description = "User to run scrolls service as.";
            type = str;
            default = scrollsName name;
          };

          group = mkOption {
            description = "Group to run scrolls service as.";
            type = str;
            default = scrollsName name;
          };

          logLevel = mkOption {
            description = "Log verbosity level";
            type = str;
            # TODO: Enum
            default = "info";
          };

          configFile = mkOption {
            description = "Path to config .toml file";
            type = path;
          };
        };
      }));
    };
  };
  config = {
    users.users = mapAttrs'
      (name: conf: nameValuePair (scrollsName name) {
        isSystemUser = true;
        group = conf.group;
      })
      enabledInstnaces;
    users.groups = mapAttrs' (name: conf: nameValuePair (scrollsName name) { }) enabledInstnaces;

    systemd.services = mapAttrs'
      (name: conf: nameValuePair (scrollsName name) {
        enable = true;
        description = "Scrolls - ${scrollsName name}";
        after = [ "postgresql.service" ];
        wantedBy = [ "multi-user.target" ];

        script = escapeShellArgs (concatLists [
          [ "${cfg.package}/bin/scrolls" "daemon" ]
          [ "--console" "plain" ]
          [ "--config" "${conf.configFile}" ]
        ]);

        environment = {
          RUST_LOG = conf.logLevel;
        };

        serviceConfig = {
          User = conf.user;
          Group = conf.user;
          Restart = "always";
          RuntimeDirectory = scrollsName name;
          StateDirectory = scrollsName name;
          # TODO: Finish it
        };
      })
      enabledInstnaces;
  };
}
