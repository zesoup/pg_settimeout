# contrib/worker_spi/Makefile

MODULE_big = pg_settimeout
OBJS =  pg_settimeout.o

EXTENSION = pg_settimeout
DATA = pg_settimeout--1.0.sql

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_settimeout
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
