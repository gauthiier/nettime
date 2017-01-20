# generates docs

ARCHIVE := arch/nettime-l_2016-12-31.json.gz
CMDSCRIPT := command-script.json

all: init index.html txt.txt clean-stage

init:
	 @command -v pandoc > /dev/null 2>&1 || (echo 'pandoc not found... please visit -- http://johnmacfarlane.net/pandoc/installing.html' && exit 1)

%.md.stage: index.md
	python ../report.py -i $(CMDSCRIPT) -o $@ -t $< -a $(ARCHIVE)

%.html: %.md.stage
	pandoc -s --template style/template.html5 -c style/style.css -o $@ $<

%.txt.stage: txt.d
	python ../report.py -i $(CMDSCRIPT) -o $@ -t $< -a $(ARCHIVE)

%.txt:  %.txt.stage
	fold -sw 80 $< > $@

clean-stage:
	rm -rf  *.stage

clean:
	rm -rf  *.hml *.txt	

.PHONY: init clean clean-stage