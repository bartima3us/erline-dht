start:
	rebar3 shell --config=config/sys.config --sname erlinedht

tests:
	rebar3 eunit
	rebar3 ct --verbose --sys_config=test/sys.config --sname erlinedht-test