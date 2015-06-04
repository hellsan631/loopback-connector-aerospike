

function operators(stream)
    --
    -- clone a table. creates a shallow copy of the table.
    --
    local function clone_table(t)
        local out = {}
        for k,v in pairs(t) do
            out[k] = v
        end
        return out
    end

    --
    -- Clone a value.
    --
    local function clone(v)

        local t = type(v)

        if t == 'number' then
            return v
        elseif t == 'string' then
            return v
        elseif t == 'boolean' then
            return v
        elseif t == 'table' then
            return clone_table(v)
        elseif t == 'userdata' then
            if v.__index == Map then
                return map.clone(v)
            elseif v.__index == List then
                return list.clone(v)
            end
            return nil
        end

        return v
    end
    local function map_example(record)

        local touple = clone(record)

        return touple
    end
    local function filter_example(record)
        debug("This is the operations array: ", tostring(record))

        return true
    end

    return stream : filter(filter_example) : map(map_example)
end




function count(stream)
    local function one(rec)
        return 1;
    end

    local function add(a, b)
        return a + b;
    end
    return stream : map(one) : reduce(add);
end
