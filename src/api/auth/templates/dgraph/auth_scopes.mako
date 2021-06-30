${variable} as var(func: has(${dgraph_table}.typed)) @filter(
% for index, filter in enumerate(dgraph_filters):
    ${print_filter(filter=filter)}
    ${'OR' if index < len(dgraph_filters) - 1 else ''}
% endfor
)
<%def name="print_filter(filter)">
(
% for index, condition in enumerate(filter):
eq(${condition[0]}, ${condition[1]}) ${'AND' if index < len(filter) - 1 else ''}
% endfor
)
</%def>