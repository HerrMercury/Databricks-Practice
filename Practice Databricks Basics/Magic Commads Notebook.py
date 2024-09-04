# Databricks notebook source
# MAGIC %sql
# MAGIC select 1+2 as sum

# COMMAND ----------

print('Hello')

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC print("Hello")

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %md
# MAGIC ####Available line magics:
# MAGIC %alias  %alias_magic  %autoawait  %autocall  %automagic  %autosave  %bookmark  %cat  %cd  %clear  %code_wrap  %colors  %conda  %config  %connect_info  %cp  %debug  %dhist  %dirs  %doctest_mode  %ed  %edit  %env  %gui  %hist  %history  %killbgscripts  %ldir  %less  %lf  %lk  %ll  %load  %load_ext  %loadpy  %logoff  %logon  %logstart  %logstate  %logstop  %ls  %lsmagic  %lx  %macro  %magic  %man  %matplotlib  %mkdir  %more  %mv  %notebook  %page  %pastebin  %pdb  %pdef  %pdoc  %pfile  %pinfo  %pinfo2  %pip  %popd  %pprint  %precision  %prun  %psearch  %psource  %pushd  %pwd  %pycat  %pylab  %qtconsole  %quickref  %recall  %rehashx  %reload_ext  %rep  %rerun  %reset  %reset_selective  %restart_python  %rm  %rmdir  %run  %save  %sc  %set_env  %store  %sx  %system  %tb  %time  %timeit  %unalias  %unload_ext  %who  %who_ls  %whos  %xdel  %xmode
# MAGIC
# MAGIC ####Available cell magics:
# MAGIC %%!  %%HTML  %%SVG  %%bash  %%capture  %%code_wrap  %%debug  %%file  %%html  %%javascript  %%js  %%latex  %%markdown  %%perl  %%prun  %%pypy  %%python  %%python2  %%python3  %%ruby  %%script  %%sh  %%svg  %%sx  %%system  %%time  %%timeit  %%writefile
# MAGIC
# MAGIC Automagic is ON, % prefix IS NOT needed for line magics.

# COMMAND ----------

# MAGIC %fs ls
