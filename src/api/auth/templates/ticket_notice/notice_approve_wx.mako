## -*- coding: utf-8 -*-
<%
    header_tip = u'【IEG数据平台】您有权限申请单据需要处理'
%>
${header_tip}

% for key, value in all_content.items():
% if key != 'scope':
%if value:
${display_dict.get(key)}：${value}
%endif
%else:
%if value != []:
${display_dict.get(key)}：
    % for val in value:
    ${val}
    %endfor
%endif
% endif
% endfor