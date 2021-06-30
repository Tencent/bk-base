<html>
    <header>
        <meta http-equiv='Content-Type' content='text/html; charset=utf-8'>
        <title>数据平台权限模块角色配置</title>
        <meta name='description' content=''>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-eOJMYsd53ii+scO/bJGFsiCZc+5NDVN2yr8+0RDqr0Ql0h+rP48ckxlpbzKgwra6" crossorigin="anonymous">
    </header>
    <body style="margin:50px;">
        <div>
            <table class="table">
                <thead>
                    <tr>
                        <th></th>
                        % for role in roles:
                        <th>${role['role_name']} (${role['role_id']})</th>
                        % endfor
                    </tr>
                </thead>
                <tbody>
                    % for action in actions:
                    <tr>
                        <td>${action['action_name']}(${action['action_id']})</td>
                        % for role in roles:
                        <td>${'YES' if action['action_id'] in role['actions'] else '-'}</td>
                        %endfor
                    </tr>
                    % endfor
                </tbody>
            </table>
        </div>
    </body>
</html>