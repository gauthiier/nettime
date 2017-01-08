# generates docs

ARCHIVE := arch/nettime-l_2016-12-31.json.gz
CMDSCRIPT := command-script.json

all: init index.html clean-stage

init:
	 @command -v pandoc > /dev/null 2>&1 || (echo 'pandoc not found... please visit -- http://johnmacfarlane.net/pandoc/installing.html' && exit 1)

%.stage.md: index.md
	python ../report.py -i $(CMDSCRIPT) -o index.stage.md -t index.md -a $(ARCHIVE)

%.html: %.stage.md
	pandoc -s --template style/template.html5 -c style/style.css -o $@ $<

clean-stage:
	rm -rf  *.stage.md

clean:
	rm -rf  *.hml *.txt	

.PHONY: init clean clean-stage