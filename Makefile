REBAR = $(shell which rebar3)

app:
	@$(REBAR) compile

clean: clean-docs
	rm -rf _build
	rm -rf rebar.lock
	rm -f erl_crash.dump

docs: clean-docs
	@$(REBAR) edoc

clean-docs:
	rm -f doc/*.css
	rm -f doc/*.html
	rm -f doc/*.png
	rm -f doc/edoc-info

# Tests
tests: app eunit ct

eunit:
	$(REBAR) as test eunit

ct: app
	$(REBAR) as test ct

# Dialyzer.
.mongodb-erlang.plt: 
	dialyzer --build_plt --output_plt .mongodb-erlang.plt \
		--apps erts kernel stdlib sasl inets crypto public_key ssl mnesia syntax_tools asn1

dialyzer: .mongodb-erlang.plt
	dialyzer -I include -I _build/default/lib/ --src -r src --plt .mongodb-erlang.plt --no_native \
		-Werror_handling -Wunmatched_returns

.PHONY: app clean docs clean-docs tests eunit ct dialyzer
