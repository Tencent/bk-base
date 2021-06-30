% if extra_fields:
{
    var(func: eq(ProjectInfo.project_id, ${project_id})) {
        ~ProjectRawData.project 
            % if bk_biz_id is not UNDEFINED:
            @filter(eq(ProjectRawData.bk_biz_id, ${bk_biz_id}))
            % endif
        {
            rd_uids as ProjectRawData.raw_data
        }
    }
    
    content(func: uid(rd_uids)) {
      	bk_biz_id: AccessRawData.bk_biz_id
      	raw_data_id: AccessRawData.id
      	raw_data_name: AccessRawData.raw_data_name
      	description: AccessRawData.description
      	created_by
      	created_at
      	updated_by
      	updated_at 
    }
}
% else:
{
    content(func: eq(ProjectRawData.project_id, ${project_id})) 
        % if bk_biz_id is not UNDEFINED:
        @filter(eq(ProjectRawData.bk_biz_id, ${bk_biz_id}))
        % endif
    {
        bk_biz_id: ProjectRawData.bk_biz_id
        raw_data_id: ProjectRawData.raw_data_id
    }
}
% endif