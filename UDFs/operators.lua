function operators(stream,operations)

    -- operations looks like: [{operator: 'not_equal', key: 'name', value: 'apperson'}]
    local tempStream = stream
    local currentOP

    local _Operators = {
        not_equal = function(record)
            info("Hello from foo bar")

            -- somehow also pass in the key / value of the operation
            return record[currentOP[key]] ~= currentOP[value]
        end
    };

    for i, operation in ipairs(operations) do
        currentOP = operation

        tempStream = tempStream:filter(_Operators[operation.operator])
    end

    return tempStream
end
