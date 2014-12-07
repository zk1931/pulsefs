all: venv
	. venv/bin/activate && pep8 *test*.py
	. venv/bin/activate && py.test --ignore=venv -s -q

get-pip.py:
	wget https://bootstrap.pypa.io/get-pip.py

~/.local/bin/pip: get-pip.py
	python get-pip.py --user

clean:
	rm -rf get-pip.py venv *.pyc __pycache__

venv: ~/.local/bin/pip requirements.txt
	pip install --user virtualenv
	test -d venv || ~/.local/bin/virtualenv venv
	. venv/bin/activate && pip install -r requirements.txt
