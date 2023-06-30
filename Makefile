.PHONY: db_init db_start db_stop psql requires_nix_shell

db_init: requires_nix_shell
	mkdir -p db
	initdb -D db
	echo "unix_socket_directories = '.'" >> db/postgresql.conf
	pg_ctl -D db -l logfile start
	createuser -h localhost scrolls -s
	createdb -h localhost scrolls

db_start: requires_nix_shell
	pg_ctl -D db -l logfile start

db_stop: requires_nix_shell
	pg_ctl -D db -l logfile stop

psql: requires_nix_shell
	psql -h localhost -U scrolls

requires_nix_shell:
	@ [ "$(IN_NIX_SHELL)" ] || echo "The $(MAKECMDGOALS) target must be run from inside a nix shell"
	@ [ "$(IN_NIX_SHELL)" ] || (echo "    run 'nix develop' first" && false)
