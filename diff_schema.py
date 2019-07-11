#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: zeonto
# @Date:   2019-07-12 02:13:28
# @Last Modified by:   zeonto
# @Last Modified time: 2019-07-12 02:15:06

import sys, time, re
try:
        from optparse import OptionParser
        import os
except ImportError:
        print >> sys.stderr, """\

There was a problem importing one of the Python modules required to run yum.
The error leading to this problem was:

%s

Please install a package which provides this module, or
verify that the module is installed correctly.

It's possible that the above module doesn't match the current version of Python,
which is:

%s

""" % (sys.exc_value, sys.version)
        sys.exit(1)

__prog__= "merge_schema"
__version__="0.1-beta"


def config_option():
    usage =  "%prog [options] arg \n"
    usage += " 示例: %prog -s source_schema.sql -t target_schema.sql -o diff_schema.sql"
    parser = OptionParser(usage)
    parser.add_option("-t","--target",dest="target_schema",help="from database schema file")
    parser.add_option("-s","--source",dest="source_schema",help="to database schema file")
    parser.add_option("-o","--out",dest="diff_alters",help="output diff alters")

    (options, args) = parser.parse_args()

    if not options.target_schema or not options.source_schema or not options.diff_alters:
        parser.error("必须输入参数：-s、-t、-o");

    global opt_main
    opt_main = {}
    opt_main["target_schema"] = options.target_schema
    opt_main["source_schema"] = options.source_schema
    opt_main["diff_alters"] = options.diff_alters

class SchemaObjects(object):
    def __init__(self,target_schema,source_schema):
        self.target_schema = target_schema
        self.source_schema   = source_schema
        self.run()

    def run(self):
        self.objects_alters = ''
        self.return_objects = {}
        self.return_objects['tables'] = {}
        self.return_objects['servers'] = {}
        self.return_objects['events'] = {}
        self.return_objects['routines'] = {}
        self.return_objects['triggers'] = {}

        self.target_tables = self._get_tables(self.target_schema)
        self.source_tables = self._get_tables(self.source_schema)
        self.diff_tables = self._get_diff_tables(self.target_tables,self.source_tables)
        for table in self.diff_tables:
            self.return_objects['tables'][table] = {}
            self.return_objects['tables'][table]['target_table'] = self._get_table_definitions(self.diff_tables[table]['target_table'])
            self.return_objects['tables'][table]['source_table']   = self._get_table_definitions(self.diff_tables[table]['source_table'])

    def _record_alters(self,alter):
        self.objects_alters += alter
        self.objects_alters += "\n"
        print alter

    def get_objects_alters(self):
        return self.objects_alters

    def get_schema_objects(self):
        return self.return_objects

    def _get_servers(self,schema_name):
        pass

    def _get_events(self,schema_name):
        pass

    def _get_routines(self,schema_name):
        pass

    def _get_triggers(self,schema_name):
        pass

    def _get_tables(self,schema_name):
        try:
            schema_file = open(schema_name, 'r')
        except IOError:
            print 'Cannot open file', schema_name
        else:
            schema_file.readline()
            schema_string = ''
            for line in schema_file:
                schema_string = schema_string + line
            schema_file.close()
            return_tables = {}
            tables = re.findall(r"CREATE TABLE[^;]*;", schema_string)
            for table in tables:
                table_name = re.match(r"(CREATE TABLE \`)(.*)(\` \()", table)
                if table_name:
                    return_tables[table_name.group(2)] = table

            return return_tables

    def _get_diff_tables(self,target_tables,source_tables):
        return_tables = {}
        if target_tables and source_tables:
            for table in target_tables:
                if source_tables.has_key(table):
                    if target_tables[table] == source_tables[table]:
                        pass
                    else:
                        return_tables[table] = {}
                        return_tables[table]['target_table'] = target_tables[table]
                        return_tables[table]['source_table'] = source_tables[table]
                else:
                     self._record_alters("-- %s" % (table))
                     self._record_alters("drop table %s;" % (table))
                     self._record_alters(" ")

            for table in source_tables:
                if target_tables.has_key(table):
                    pass
                else:
                    self._record_alters("-- %s" % (table))
                    self._record_alters("%s" % (source_tables[table]))
                    self._record_alters(" ")

        return return_tables

    def _get_table_definitions(self,schema_table):
        return_definitions = {}
        return_definitions['column'] = {}
        return_definitions['primary'] = {}
        return_definitions['unique'] = {}
        return_definitions['key'] = {}
        return_definitions['foreign'] = {}
        return_definitions['fulltext'] = {}
        return_definitions['option'] = {}
        return_definitions['column_position'] = {}

        table_definitions = schema_table.split('\n')

        for definition in table_definitions:
            column_name = re.match(r"(\s*\`)([^`]*)(\`.*)", definition)
            if column_name:
                tmp = column_name.group().split(",")
                _column_content = ",".join(tmp[:-1])
                return_definitions['column'][column_name.group(2)] = _column_content.strip()
                return_definitions['column_position'][column_name.group(2)] = table_definitions.index(definition)

            primary_name = re.match(r"(\s*PRIMARY KEY\s*)", definition)
            if primary_name:
                return_definitions['primary']['primary'] = re.match(r"(\s*)(PRIMARY KEY \(.*\))(,?)", definition).group(2)

            unique_name = re.match(r"(\s*UNIQUE KEY \`)([^`]*)(\`.*)", definition)
            if unique_name:
                return_definitions['unique'][unique_name.group(2)] = re.match(r"(\s*)(UNIQUE KEY.*\))(,?)", definition).group(2)

            key_name = re.match(r"(\s*KEY \`)([^`]*)(\`.*)", definition)
            if key_name:
                return_definitions['key'][key_name.group(2)] = re.match(r"(\s*)(KEY.*\))(,?)", definition).group(2)

            foreign_name = re.match(r"(\s*CONSTRAINT \`)([^`]*)(\`.*)", definition)
            if foreign_name:
                return_definitions['foreign'][foreign_name.group(2)] = re.match(r"(\s*)(CONSTRAINT[^,]*)(,?)", definition).group(2)

            fulltext_name = re.match(r"(\s*FULLTEXT KEY \`)([^`]*)(\`.*)", definition)
            if fulltext_name:
                return_definitions['fulltext'][fulltext_name.group(2)] = re.match(r"(\s*)(FULLTEXT KEY.*\))(,?)", definition).group(2)

            option_name = re.match(r"(\)\s*ENGINE=.*)", definition)
            if option_name:
                pattern = re.compile(r' AUTO_INCREMENT=\d+| ROW_FORMAT=\w+', re.I)
                engine_content = re.sub(pattern, '', re.match(r"(\)\s*)(ENGINE[^;]*)(;?)", definition).group(2))
                return_definitions['option']['option'] = engine_content

        return return_definitions

class SchemaAlters(object):
    def __init__(self,schema_objects):
        self.diff_objects = schema_objects
        self.run()

    def run(self):
        self.definitions_alters = ''
        self.return_alters = {}
        self.return_alters['tables'] = {}
        self.return_alters['servers'] = {}
        self.return_alters['events'] = {}
        self.return_alters['routines'] = {}
        self.return_alters['triggers'] = {}
        self._alter_tables(self.diff_objects['tables'])

    def _record_alters(self,alter):
        self.definitions_alters += alter
        self.definitions_alters += "\n"
        print alter

    def get_definitions_alters(self):
        return self.definitions_alters

    def _alter_tables(self,schema_tables):
        for table in schema_tables:
            self._record_alters("-- %s" % (table))
            target_table = schema_tables[table]['target_table']
            source_table = schema_tables[table]['source_table']

            self._column(table,target_table['column'],source_table['column'])
            self._primary(table,target_table['primary'],source_table['primary'])
            self._unique(table,target_table['unique'],source_table['unique'])
            self._key(table,target_table['key'],source_table['key'])
            self._foreign(table,target_table['foreign'],source_table['foreign'])
            self._fulltext(table,target_table['fulltext'],source_table['fulltext'])
            self._option(table,target_table['option'],source_table['option'])
            self._record_alters(" ")

    def _get_option_diff(self, source_option, target_option):
        """
        @brief      获取表设置差异
        
        @param      self           The object
        @param      source_option  The source option
        @param      target_option  The target option
        
        @return     The option difference.
        """
        check_option = ['ENGINE', 'CHARSET', 'COMMENT'] # 指定检查表设置项
        sources = source_option.split(' ')
        targets = target_option.split(' ')
        _sources = {}
        _targets = {}
        for target_item in targets:
            if target_item:
                _item = target_item.split('=', 1)
                if len(_item) == 2 and _item[0] in check_option:
                    _targets[_item[0]] = _item[1]

        for source_item in sources:
            if source_item:
                _item = source_item.split('=', 1)
                if len(_item) == 2 and _item[0] in check_option:
                    _sources[_item[0]] = _item[1]

        option_diff = ''
        for option_member in check_option:
            if option_member in _sources.keys() and option_member in _targets.keys() and _sources[option_member] == _targets[option_member]:
                pass
            else:
                option_diff += option_member + '=' +  _sources[option_member] + ' '
        return option_diff.strip()

    def _get_column_position_num(self,column_position,column):
        """
        @brief      获取字段所在位置（数字）
        
        @param      self             The object
        @param      column_position  The column position
        @param      column           The column
        
        @return     The column position number.
        """
        if (column_position[column]):
            return column_position[column]
        return 0

    def _get_next_column(self,position_dict,column):
        """
        @brief      获取下一个字段
        
        @param      self           The object
        @param      position_dict  The position dictionary
        @param      column         The column
        
        @return     The next column.
        """
        if position_dict[column] == len(position_dict):
            # 最后一个字段没有下一个字段，返回空
            return ''
        next_position = position_dict[column] + 1
        next_column = list(position_dict.keys())[list(position_dict.values()).index(next_position)]
        return next_column

    def _get_before_column(self,position_dict,column):
        """
        @brief      获取上一个字段
        
        @param      self           The object
        @param      position_dict  The position dictionary
        @param      column         The column
        
        @return     The before column.
        """
        if position_dict[column] == 1:
            # 第1个字段前面没有字段
            return ''
        before_position = position_dict[column] - 1
        before_column = list(position_dict.keys())[list(position_dict.values()).index(before_position)]
        return before_column

    def _get_target_next_column(self,source_position_dict,target_position_dict,column):
        """
        @brief      获取目标结构中的下一个字段
        
        @param      self                  The object
        @param      source_position_dict  The source position dictionary
        @param      target_position_dict  The target position dictionary
        @param      column                The column
        
        @return     The target next column.
        """
        source_position = source_position_dict[column];
        if source_position < len(source_position_dict):
            for x in xrange(source_position+1,len(source_position_dict)+1):
                next_column = list(source_position_dict.keys())[list(source_position_dict.values()).index(x)]
                if target_position_dict.has_key(next_column):
                    return next_column
        return ''

    def _get_source_before_column(self,source_position_dict,target_position_dict,column):
        """
        @brief      从源结构中获取前面一个字段
        
        @param      self                  The object
        @param      source_position_dict  The source position dictionary
        @param      target_position_dict  The target position dictionary
        @param      column                The column
        
        @return     The source before column.
        """
        source_position = source_position_dict[column];
        # 从第1个参数开始，到第2个参数之前结束
        # 按位置从后往前查，字段前面一个字段存在目标结构中，则返回该字段
        for x in xrange(source_position-1,0,-1):
            before_column = list(source_position_dict.keys())[list(source_position_dict.values()).index(x)]
            return before_column
        return ''

    def _get_target_before_column(self,source_position_dict,target_position_dict,column):
        """
        @brief      从目标结构获取前面一个字段
        
        @param      self                  The object
        @param      source_position_dict  The source position dictionary
        @param      target_position_dict  The target position dictionary
        @param      column                The column
        
        @return     The target before column.
        """
        source_position = source_position_dict[column];
        # 从第1个参数开始，到第2个参数之前结束
        # 按位置从后往前查，字段前面一个字段存在目标结构中，则返回该字段
        for x in xrange(source_position-1,0,-1):
            before_column = list(source_position_dict.keys())[list(source_position_dict.values()).index(x)]
            if target_position_dict.has_key(before_column):
                return before_column
        return ''

    def _get_column_position_sql(self,source_position_dict,target_position_dict,column):
        """
        @brief      获取字段所在位置关系
        
        @param      self                  The object
        @param      source_position_dict  The source position dictionary {'status': 5, 'id': 1}
        @param      target_position_dict  The target position dictionary
        @param      column                The column
        
        @return     The column position sql.
        """
        # if (source_position_dict[column] and target_position_dict[column]):
        if (source_position_dict.has_key(column) and target_position_dict.has_key(column)):
            if (source_position_dict[column] == target_position_dict[column]):
                return ''
            else:
                current_postion = source_position_dict[column]
                if current_postion == 1:
                    # 假如是第一个字段
                    return ' FIRST'
                before_column = self._get_target_before_column(source_position_dict, target_position_dict, column)
                if before_column:
                        return " AFTER `%s`" % (before_column)
                # if current_postion > 1:
                #     # 取出当前字段的上一个字段位置，然后用位置获取对应的字段名
                #     before_position = current_postion - 1
                #     before_column = list(source_position_dict.keys())[list(source_position_dict.values()).index(before_position)]
                #     if before_column:
                #         return " AFTER `%s`" % (before_column)
                #     else:
                #         return ''
                # else:
                #     return ' FIRST '
                #     next_position = current_postion + 1
                #     next_column = list(source_position_dict.keys())[list(source_position_dict.values()).index(next_position)]
                #     if next_column:
                #         return " BEFORE `%s`" % (next_column)
                #     else:
                #         return ''
        return ''

    def _column(self,table,target_column,source_column):
        source_position_dict = self.diff_objects['tables'][table]['source_table']['column_position']
        target_position_dict = self.diff_objects['tables'][table]['target_table']['column_position']

        colimn_sql = "ALTER TABLE `%s`\n" % (table)
        for definition in target_column:
            if source_column.has_key(definition):
                source_position = self._get_column_position_num(source_position_dict, definition)
                target_position = self._get_column_position_num(target_position_dict, definition)
                if source_position == target_position and target_column[definition] == source_column[definition]:
                    # 字段内容、字段位置一致，没变化跳过
                    pass
                else:
                    # 字段内容没变化，字段位置改变
                    source_before_column = self._get_before_column(source_position_dict, definition)
                    target_before_column = self._get_before_column(target_position_dict, definition)
                    if target_column[definition] == source_column[definition] and source_before_column == target_before_column:
                        # 字段内容没变化，上一个字段也相同，跳过
                        pass
                    else:
                        source_next_column = self._get_next_column(source_position_dict, definition)
                        target_next_column = self._get_next_column(target_position_dict, definition)
                        if target_column[definition] == source_column[definition] and source_next_column == target_next_column:
                            # 字段内容没变化，下一个字段也相同，跳过
                            pass
                        else:
                            column_position = self._get_column_position_sql(source_position_dict, target_position_dict, definition)
                            colimn_sql += "\tMODIFY COLUMN %s,\n" % (source_column[definition] + column_position) 
            else:
                colimn_sql += ("\tDROP COLUMN `%s`,\n" % (definition))

        for definition in source_column:
            if target_column.has_key(definition):
                pass
            else:
                target_before_column = self._get_target_before_column(source_position_dict, target_position_dict, definition)
                colimn_sql += ("\tADD COLUMN %s AFTER %s,\n" % (source_column[definition],target_before_column))
        # 同表字段操作拼接一条语句
        self._record_alters(colimn_sql.strip('\n').strip(',') + ';')

    def _primary(self,table,from_primary,to_primary):
        if from_primary.has_key('primary'):
            if to_primary.has_key('primary'):
                if from_primary['primary'] == to_primary['primary']:
                    pass
                else:
                    self._record_alters("alter table `%s` drop primary key;" % (table))
                    self._record_alters("alter table `%s` add %s;" % (table,to_primary['primary']))
            else:
                self._record_alters("alter table `%s` drop primary key;" % (table))

        if to_primary.has_key('primary'):
            if from_primary.has_key('primary'):
                pass
            else:
                self._record_alters("alter table `%s` add %s;" % (table,to_primary['primary']))

    def _unique(self,table,from_unique,to_unique):
        for definition in from_unique:
            if to_unique.has_key(definition):
                if from_unique[definition] == to_unique[definition]:
                    pass
                else:
                    self._record_alters("alter table `%s` drop unique key %s;" % (table,definition))
                    self._record_alters("alter table `%s` add %s;" % (table,to_unique[definition]))
            else:
                self._record_alters("alter table `%s` drop unique key %s;" % (table,definition))

        for definition in to_unique:
            if from_unique.has_key(definition):
                pass
            else:
                self._record_alters("alter table `%s` add %s;" % (table,to_unique[definition]))

    def _key(self,table,from_key,to_key):
        _sql = "ALTER TABLE `%s`\n" % (table)
        for definition in from_key:
            if to_key.has_key(definition):
                if from_key[definition] == to_key[definition]:
                    pass
                else:
                    _sql += "\tDROP KEY %s,\n" % (definition)
                    _sql += "\tADD %s,\n" % (to_key[definition])
            else:
                _sql += "\tDROP KEY %s,\n" % (definition)

        for definition in to_key:
            if from_key.has_key(definition):
                pass
            else:
                _sql += "\tADD %s,\n" % (to_key[definition])

        self._record_alters(_sql.strip('\n').strip(',') + ';')

    def _foreign(self,table,from_foreign,to_foreign):
        for definition in from_foreign:
            if to_foreign.has_key(definition):
                if from_foreign[definition] == to_foreign[definition]:
                    pass
                else:
                    self._record_alters("alter table `%s` drop foreign key `%s`;" % (table,definition))
                    self._record_alters("alter table `%s` add %s;" % (table,to_foreign[definition]))
            else:
                self._record_alters("alter table `%s` drop foreign key `%s`;" % (table,definition))

        for definition in to_foreign:
            if from_foreign.has_key(definition):
                pass
            else:
                self._record_alters("alter table `%s` add %s;" % (table,to_foreign[definition]))

    def _fulltext(self,table,from_fulltext,to_fulltext):
        for definition in from_fulltext:
            if to_fulltext.has_key(definition):
                if from_fulltext[definition] == to_fulltext[definition]:
                    pass
                else:
                    self._record_alters("alter table `%s` drop fulltext key `%s`;" % (table,definition))
                    self._record_alters("alter table `%s` add %s;" % (table,to_fulltext[definition]))
            else:
                self._record_alters("alter table `%s` drop fulltext key `%s`;" % (table,definition))

        for definition in to_fulltext:
            if from_fulltext.has_key(definition):
                pass
            else:
                self._record_alters("alter table `%s` add %s;" % (table,to_fulltext[definition]))

    def _option(self,table,from_option,to_option):
        if from_option.has_key('option'):
            if to_option.has_key('option'):
                if from_option['option'] == to_option['option']:
                    pass
                else:
                    option_content = self._get_option_diff(to_option['option'],from_option['option'])
                    self._record_alters("ALTER TABLE `%s` %s;" % (table,option_content))


def main():
    config_option()

    current_objects = SchemaObjects(opt_main["target_schema"],opt_main["source_schema"])
    schema_objects = current_objects.get_schema_objects()
    objects_alters = current_objects.get_objects_alters()

    current_alters = SchemaAlters(schema_objects)
    definitions_alters = current_alters.get_definitions_alters()

    diff_alters = open(opt_main["diff_alters"],'w')
    diff_alters.write('-- set default character\nset names utf8;\n\n')
    diff_alters.write(objects_alters)
    diff_alters.write(definitions_alters)
    diff_alters.close()

if __name__ == "__main__":
    main()
