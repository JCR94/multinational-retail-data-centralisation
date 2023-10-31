import os
import runpy

if __name__ == '__main__':
    abspath = os.path.dirname(__file__)

    # Runs the __main__.py file inside the `python_scripts` directory, including the body inside `if __name__ == '__main__':`.
    python_scripts_path = os.path.join(abspath, 'python_scripts')
    runpy.run_path(python_scripts_path, run_name='__main__')
    
    # Runs the __main__.py file inside the `sql_scripts/create_db_schema` directory, including the body inside `if __name__ == '__main__':`.
    sql_scripts_path = os.path.join(abspath, 'sql_scripts', 'create_db_schema')
    runpy.run_path(sql_scripts_path, run_name='__main__')