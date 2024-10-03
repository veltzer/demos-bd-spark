##############
# parameters #
##############
# do you want to show the commands executed ?
DO_MKDBG?=0
# should we depend on the Makefile itself?
DO_ALLDEP:=1
# do you want to check bash syntax?
DO_SHELLCHECK:=1
# do spell check on all?
DO_MD_ASPELL:=1
# do you want to run mdl on md files?
DO_MD_MDL:=1

########
# code #

ALL_SH:=$(shell find . -type f -name "*.sh" -and -not -path "./.venv/*" -printf "%P\n")
ALL_SHELLCHECK:=$(addprefix out/, $(addsuffix .shellcheck, $(ALL_SH)))

MD_SRC:=$(shell find exercises -type f -and -name "*.md")
MD_BAS:=$(basename $(MD_SRC))
MD_ASPELL:=$(addprefix out/,$(addsuffix .aspell,$(MD_BAS)))
MD_MDL:=$(addprefix out/,$(addsuffix .mdl,$(MD_BAS)))

ifeq ($(DO_SHELLCHECK),1)
ALL+=$(ALL_SHELLCHECK)
endif # DO_SHELLCHECK

ifeq ($(DO_MD_ASPELL),1)
ALL+=$(MD_ASPELL)
endif # DO_MD_ASPELL

ifeq ($(DO_MD_MDL),1)
ALL+=$(MD_MDL)
endif # DO_MD_MDL

# silent stuff
ifeq ($(DO_MKDBG),1)
Q:=
# we are not silent in this branch
else # DO_MKDBG
Q:=@
#.SILENT:
endif # DO_MKDBG

#########
# rules #
#########
.PHONY: all
all: $(ALL)
	@true

.PHONY: clean
clean:
	$(info doing [$@])
	$(Q)-rm -f $(ALL)

.PHONY: clean_hard
clean_hard:
	$(info doing [$@])
	$(Q)git clean -qffxd

.PHONY: debug
debug:
	$(info ALL is $(ALL))
	$(info ALL_SH is $(ALL_SH))
	$(info ALL_SHELLCHECK is $(ALL_SHELLCHECK))
	$(info MD_SRC is $(MD_SRC))
	$(info MD_ASPELL is $(MD_ASPELL))
	$(info MD_MDL is $(MD_MDL))

.PHONY: spell_many
spell_many:
	$(info doing [$@])
	$(Q)aspell_many.sh $(MD_SRC)

############
# patterns #
############
$(ALL_SHELLCHECK): out/%.shellcheck: % .shellcheckrc
	$(info doing [$@])
	$(Q)shellcheck --shell=bash --external-sources --source-path="$$HOME" $<
	$(Q)pymakehelper touch_mkdir $@
$(MD_ASPELL): out/%.aspell: %.md .aspell.conf .aspell.en.prepl .aspell.en.pws
	$(info doing [$@])
	$(Q)aspell --conf-dir=. --conf=.aspell.conf list < $< | pymakehelper error_on_print sort -u
	$(Q)pymakehelper touch_mkdir $@
$(MD_MDL): out/%.mdl: %.md .mdlrc .mdl.style.rb
	$(info doing [$@])
	$(Q)GEM_HOME=gems gems/bin/mdl $<
	$(Q)pymakehelper touch_mkdir $@

##########
# alldep #
##########
ifeq ($(DO_ALLDEP),1)
.EXTRA_PREREQS+=$(foreach mk, ${MAKEFILE_LIST},$(abspath ${mk}))
endif # DO_ALLDEP
