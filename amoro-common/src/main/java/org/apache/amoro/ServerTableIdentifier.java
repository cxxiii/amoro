/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro;

import org.apache.amoro.table.TableIdentifier;

import java.util.Objects;

/** 服务端表标识符，包含服务端ID和表格式信息 */
public class ServerTableIdentifier {

  private Long id; // 服务端表ID
  private String catalog; // 目录名称
  private String database; // 数据库名称
  private String tableName; // 表名称
  private TableFormat format; // 表格式

  // 供MyBatis框架使用的无参构造方法
  private ServerTableIdentifier() {}

  /**
   * 通过TableIdentifier和表格式构造ServerTableIdentifier
   *
   * @param tableIdentifier 表标识符
   * @param format 表格式
   */
  private ServerTableIdentifier(TableIdentifier tableIdentifier, TableFormat format) {
    this.catalog = tableIdentifier.getCatalog();
    this.database = tableIdentifier.getDatabase();
    this.tableName = tableIdentifier.getTableName();
    this.format = format;
  }

  /**
   * 通过目录、数据库、表名和表格式构造ServerTableIdentifier
   *
   * @param catalog 目录名称
   * @param database 数据库名称
   * @param tableName 表名称
   * @param format 表格式
   */
  private ServerTableIdentifier(
      String catalog, String database, String tableName, TableFormat format) {
    this.catalog = catalog;
    this.database = database;
    this.tableName = tableName;
    this.format = format;
  }

  /**
   * 通过ID、目录、数据库、表名和表格式构造ServerTableIdentifier
   *
   * @param id 服务端表ID
   * @param catalog 目录名称
   * @param database 数据库名称
   * @param tableName 表名称
   * @param format 表格式
   */
  private ServerTableIdentifier(
      Long id, String catalog, String database, String tableName, TableFormat format) {
    this.id = id;
    this.catalog = catalog;
    this.database = database;
    this.tableName = tableName;
    this.format = format;
  }

  // 以下是各属性的getter和setter方法
  public Long getId() {
    return id;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public TableFormat getFormat() {
    return this.format;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setFormat(TableFormat format) {
    this.format = format;
  }

  /**
   * 重写equals方法，比较两个ServerTableIdentifier是否相等
   *
   * @param o 要比较的对象
   * @return 如果相等返回true，否则返回false
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerTableIdentifier that = (ServerTableIdentifier) o;
    return Objects.equals(id, that.id)
        && Objects.equals(catalog, that.catalog)
        && Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName);
  }

  /**
   * 重写hashCode方法
   *
   * @return 对象的哈希值
   */
  @Override
  public int hashCode() {
    return Objects.hash(id, catalog, database, tableName);
  }

  /**
   * 重写toString方法
   *
   * @return 对象的字符串表示形式
   */
  @Override
  public String toString() {
    return String.format("%s.%s.%s(tableId=%d)", catalog, database, tableName, id);
  }

  /**
   * 静态工厂方法，通过TableIdentifier和表格式创建ServerTableIdentifier
   *
   * @param tableIdentifier 表标识符
   * @param format 表格式
   * @return 新的ServerTableIdentifier实例
   */
  public static ServerTableIdentifier of(TableIdentifier tableIdentifier, TableFormat format) {
    return new ServerTableIdentifier(tableIdentifier, format);
  }

  /**
   * 静态工厂方法，通过目录、数据库、表名和表格式创建ServerTableIdentifier
   *
   * @param catalog 目录名称
   * @param database 数据库名称
   * @param tableName 表名称
   * @param format 表格式
   * @return 新的ServerTableIdentifier实例
   */
  public static ServerTableIdentifier of(
      String catalog, String database, String tableName, TableFormat format) {
    return new ServerTableIdentifier(catalog, database, tableName, format);
  }

  /**
   * 静态工厂方法，通过ID、目录、数据库、表名和表格式创建ServerTableIdentifier
   *
   * @param id 服务端表ID
   * @param catalog 目录名称
   * @param database 数据库名称
   * @param tableName 表名称
   * @param format 表格式
   * @return 新的ServerTableIdentifier实例
   */
  public static ServerTableIdentifier of(
      Long id, String catalog, String database, String tableName, TableFormat format) {
    return new ServerTableIdentifier(id, catalog, database, tableName, format);
  }

  /**
   * 获取TableIdentifier对象
   *
   * @return 包含目录、数据库和表名的TableIdentifier对象
   */
  public TableIdentifier getIdentifier() {
    return TableIdentifier.of(catalog, database, tableName);
  }
}
