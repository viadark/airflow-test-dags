from jinja2 import Environment, FileSystemLoader
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('dag_template')

for i in range(1000):
    with open(f"test_dag_{i}.py", "w") as file:
        file.write(template.render(
            {
                'index': i
            }
        ))