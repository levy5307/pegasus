// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "shell/commands.h"
#include "base/pegasus_utils.h"

bool load_acl_entries(shell_context *sc,
                      arguments args,
                      std::string &app_name,
                      std::string &acl_entries_str)
{
    bool ret = false;
    if (args.argc < 4)
        return ret;

    app_name = pegasus::utils::unescape_str(args.argv[1]);

    if (((args.argc - 2) & 0x01) == 1) {
        // key & value count must equal 2*n(n >= 1)
        fprintf(stderr, "need speficy the value for key = %s\n", args.argv[args.argc - 1]);
        return false;
    }

    int idx = 2;
    std::string user, permission;
    std::stringstream ss;
    while (idx < args.argc) {
        user = pegasus::utils::unescape_str(args.argv[idx++]);
        permission = pegasus::utils::unescape_str(args.argv[idx++]);
        ss << user << ":" << permission << ";";
    }
    ss >> acl_entries_str;
    fprintf(stderr,
            "LOAD: app_name \"%s\", acl_entries \"%s\"\n",
            pegasus::utils::c_escape_string(app_name, sc->escape_all).c_str(),
            pegasus::utils::c_escape_string(acl_entries_str, sc->escape_all).c_str());

    return ret;
}

bool set_acl(command_executor *e, shell_context *sc, arguments args)
{
    std::string app_name, acl_entries_str;

    if (load_acl_entries(sc, args, app_name, acl_entries_str)) {
        return false;
    }

    dsn::error_code resp;
    if (app_name == "all") {
        fprintf(stderr, "setting acl for all apps is unfinished");
    } else {
        resp = sc->ddl_client->control_acl(app_name, acl_entries_str);
    }

    if (resp == dsn::ERR_OK) {
        std::cout << "set acl ok" << std::endl;
    } else {
        std::cout << "set acl got error " << resp.to_string() << std::endl;
    }
    return true;
}

