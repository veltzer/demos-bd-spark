##############
# parameters #
##############
# do you want to show the commands executed ?
DO_MKDBG?=0
# should we depend on the Makefile itself?
DO_ALLDEP:=1
# do you want to check bash syntax?
DO_SHELLCHECK:=1


#############
# variables #
#############
ALL:=

ALL_SH:=$(shell find . -type f -name "*.sh" -and -not -path "./.venv/*" -printf "%P\n")
ALL_SHELLCHECK:=$(addprefix out/, $(addsuffix .shellcheck, $(ALL_SH)))

ifeq ($(DO_SHELLCHECK),1)
ALL+=$(ALL_SHELLCHECK)
endif # DO_SHELLCHECK

########
# code #
########
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

############
# patterns #
############
$(ALL_SHELLCHECK): out/%.shellcheck: % .shellcheckrc
	$(info doing [$@])
	$(Q)shellcheck --shell=bash --external-sources --source-path="$$HOME" $<
	$(Q)pymakehelper touch_mkdir $@

############
# all deps #
############
ifeq ($(DO_ALLDEP),1)
.EXTRA_PREREQS+=$(foreach mk, ${MAKEFILE_LIST},$(abspath ${mk}))
endif # DO_ALLDEP
