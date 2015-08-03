# Configuration variables

TARGET = hyperdex

export CROSSTEX_FLAGS:= --add-in --add-proceedings --titlecase=title --cite-by initials

# Rules that derive from configuration variables

.PHONY: all
.SUFFIXES: .gnuplot .tex

all: $(TARGET).pdf $(TARGET).ps
	@echo "SUCCESS: dvi, ps, pdf all up-to-date"

$(TARGET).dvi: hyperdex.tex $(wildcard *.tex) $(wildcard */*.tex) $(wildcard */*/*.tex)
	python2 latex.py --crosstex --index $(TARGET)

$(TARGET).ps: $(TARGET).dvi
	dvips $(TARGET).dvi

$(TARGET).pdf: $(TARGET).ps
	ps2pdf -dPDFSETTINGS=/prepress $(TARGET).ps $(TARGET).pdf

-include $(TARGET).P

clean:
	rm -f $(TARGET).aux $(TARGET).bbl $(TARGET).log $(TARGET).stdout.log .$(TARGET).xtx.cache data/*.tex data/*.aux data/*.eps

clobber: clean
	rm -f $(TARGET).dvi $(TARGET).P $(TARGET).pdf $(TARGET).ps
