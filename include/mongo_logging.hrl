-include_lib("kernel/include/logger.hrl").

-define(DEBUG(Format), ?LOG_DEBUG(Format)).
-define(DEBUG(Format, Args), ?LOG_DEBUG(Format, Args)).

-define(SECURE(Proplist), proplists:delete(password, Proplist)).
-define(TAGGED(Tag, Options), [{mongo_log_tag, Tag} | Options]).
-define(STORE_TAG_FROM_OPTS(Options), logger:set_process_metadata(
                                        #{mongo_log_tag => proplists:get_value(mongo_log_tag, Options, undefined)})).
-define(STORE_TAG(Tag), logger:set_process_metadata(
                          #{mongo_log_tag => Tag})).
