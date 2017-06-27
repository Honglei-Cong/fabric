var chaincode = {};

chaincode.init = function() {
    return shim.Success("hello, init");
}

chaincode.invoke = function() {
    var args = JSON.parse(shim.GetArguments());
    if (args.length < 2) {
        return shim.Error("invalid request, arg count " + args.length);
    }

    switch (args[0]) {
    case "put":
        if (args.length < 3) {
            return shim.Error("invalid put request, arg count " + args.length);
        }
        shim.PutState(args[1], args[2]);
        return;

    case "get":
        var result = shim.GetState(args[1]);
        if (shim.GetLastError() != null) {
            return shim.Error(shim.GetLastError());
        }
        return shim.Success(result);

    case "rangequery":
        if (args.length < 3) {
            return shim.Error("invalid range-query request, arg count " + args.length);
        }
        var ite = shim.GetStateByRange(args[1], args[2]);
        if (shim.GetLastError() != null) {
            return shim.Error(shim.GetLastError());
        }
        var count = 0;
        while (ite.HasNext()) {
            if (ite.Next()) {
                var key = ite.GetCurrentKey();
                var val = ite.GetCurrentValue();
                console.log("key", key, "value", val);
                count++
            } else {
                return shim.Error(ite.GetError());
            }
        }
        ite.Close();
        return shim.Success(count.toString());
    }
}

