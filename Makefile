.DEFAULT_GOAL := test

test:
	pylint tap_sftp -d missing-docstring,fixme,duplicate-code,line-too-long
