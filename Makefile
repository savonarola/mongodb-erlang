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
	@$(REBAR) as test eunit

ct: app
	@$(REBAR) as test ct

dialyzer:
	@$(REBAR) dialyzer

.PHONY: app clean docs clean-docs tests eunit ct dialyze
