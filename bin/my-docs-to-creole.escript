#!/usr/bin/env escript
%% -*- erlang -*-
%%! -name mdtocreole@127.0.0.1

main([File]) ->
    {ok, T} = file:read_file(File),
    R = re:replace(
        re:replace(
        re:replace(
        re:replace(
        re:replace(
        re:replace(
        re:replace(
        re:replace(
        re:replace(
        re:replace(T,
            "^# +(.*) +#$", "== \\1 ==", [global,multiline,{return,list}]),
            "^## +(.*) +##$", "=== \\1 ===", [global,multiline,{return,list}]),
            "^### +(.*) +###$", "==== \\1 ====", [global,multiline,{return,list}]),
            "^### +(.*) +###$", "==== \\1 ====", [global,multiline,{return,list}]),
            "^#### +(.*) +####$", "===== \\1 =====", [global,multiline,{return,list}]),
            "\\*([^ ][^\\*\\n]*)\\*(?=[^\\*])", "//\\1//", [global,multiline,{return,list}]),                                   % italics
            "`([^`\\n]*)`", "{{{\\1}}}", [global,multiline,{return,list}]),                                                     % type-text
            "\\[([^\\]]+)\\]\\(([^\\)]+)\\)", "[[\\2|\\1]]", [global,multiline,{return,list}]),                                 % links
            "\\n\\s*\\n((?:    \\s*[^\\n]*\\n)+)\\s*\\n", "\n\n{{{\n#!java\n\n\\1}}}\n\n", [global,multiline,{return,list}]),   % pre
            "^    ", "", [global,multiline,{return,list}]),                                                                     % -
    io:format(R);
main(_) ->
    io:format("Use ./my-docs-to-creole.sh ../README~n").

