
{
        <% level = 0 %>
        var(func: eq(ProjectInfo.project_id, ${project_id})) {
            ~ResultTable.project {
                ${cur_uids(level)} as uid
            }
            % if with_project_data:
            ~ProjectData.project {
                pd_rt_uids as ProjectData.result_table
            }
            % endif
        }

        % if with_project_data:
        <% level += 1 %>
        var(func: uid(${pre_uids(level)}, pd_rt_uids)) {
            ${cur_uids(level)} as uid
        }
        % endif

        % if bk_biz_id is not UNDEFINED:
        <% level += 1 %>
        var(func: uid(${pre_uids(level)})) @filter(eq(ResultTable.bk_biz_id, ${bk_biz_id})){
            ${cur_uids(level)} as uid
        }
        % endif

        % if not with_queryset:
        <% level += 1 %>
        var(func: uid(${pre_uids(level)})) @filter(not eq(ResultTable.processing_type, "queryset")){
            ${cur_uids(level)} as uid
        }
        % endif

        content(func: uid(${cur_uids(level)})){
            bk_biz_id: ResultTable.bk_biz_id
            result_table_id: ResultTable.result_table_id
            % if extra_fields:
            result_table_name: ResultTable.result_table_name
            description: ResultTable.description
            processing_type: ResultTable.processing_type
            generate_type: ResultTable.generate_type
            created_by
            created_at
            updated_by
            updated_at
            % endif
        }   
}

<%def name="cur_uids(level)">level${level}_uids</%def>
<%def name="pre_uids(level)">level${level - 1}_uids</%def>