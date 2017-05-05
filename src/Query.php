<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK ]
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: liu21st <liu21st@gmail.com>
// +----------------------------------------------------------------------

namespace think\mongo;

use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Cursor;
use MongoDB\Driver\Exception\AuthenticationException;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\Exception\ConnectionException;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Exception\RuntimeException;
use MongoDB\Driver\Query as MongoQuery;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;
use think\Collection;
use think\Config;
use think\Db;
use think\db\Query as BaseQuery;
use think\Exception;
use think\Model;

class Query extends BaseQuery
{
    /**
     * 架构函数
     * @access public
     */
    public function __construct(Connection $connection = null)
    {
        if (is_null($connection)) {
            $this->connection = Connection::instance();
        } else {
            $this->connection = $connection;
        }

        $this->prefix = $this->connection->getConfig('prefix');
    }

    /**
     * 切换数据库连接
     * @access public
     * @param mixed         $config 连接配置
     * @param bool|string   $name 连接标识 true 强制重新连接
     * @return $this
     * @throws Exception
     */
    public function connect($config = [], $name = false)
    {
        $this->connection = Connection::instance($config, $name);

        return $this;
    }

    /**
     * 去除某个查询条件
     * @access public
     * @param string $field 查询字段
     * @param string $logic 查询逻辑 and or xor
     * @return $this
     */
    public function removeWhereField($field, $logic = 'and')
    {
        $logic = '$' . strtoupper($logic);
        if (isset($this->options['where'][$logic][$field])) {
            unset($this->options['where'][$logic][$field]);
        }
        return $this;
    }

    /**
     * 执行查询 返回数据集
     * @access public
     * @param string $namespace
     * @param MongoQuery        $query 查询对象
     * @param ReadPreference    $readPreference readPreference
     * @param bool|string       $class 指定返回的数据集对象
     * @param string|array      $typeMap 指定返回的typeMap
     * @return mixed
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     */
    public function mongoQuery($namespace, MongoQuery $query, ReadPreference $readPreference = null, $class = false, $typeMap = null)
    {
        return $this->connection->query($namespace, $query, $readPreference, $class, $typeMap);
    }

    /**
     * 执行指令 返回数据集
     * @access public
     * @param Command           $command 指令
     * @param string            $dbName
     * @param ReadPreference    $readPreference readPreference
     * @param bool|string       $class 指定返回的数据集对象
     * @param string|array      $typeMap 指定返回的typeMap
     * @return mixed
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     */
    public function command(Command $command, $dbName = '', ReadPreference $readPreference = null, $class = false, $typeMap = null)
    {
        return $this->connection->command($command, $dbName, $readPreference, $class, $typeMap);
    }

    /**
     * 执行语句
     * @access public
     * @param string        $namespace
     * @param BulkWrite     $bulk
     * @param WriteConcern  $writeConcern
     * @return int
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function mongoExecute($namespace, BulkWrite $bulk, WriteConcern $writeConcern = null)
    {
        return $this->connection->execute($namespace, $bulk, $writeConcern);
    }

    /**
     * 获取最近插入的ID
     * @access public
     * @param string $sequence 自增序列名
     * @return string
     */
    public function getLastInsID($sequence = null)
    {
        return $this->connection->getLastInsID($sequence);
    }

    /**
     * 获取最近一次执行的指令
     * @access public
     * @return string
     */
    public function getLastSql()
    {
        return $this->connection->getQueryStr();
    }

    /**
     * 得到某个字段的值
     * @access public
     * @param string    $field 字段名
     * @param mixed     $default 默认值
     * @return mixed
     */
    public function value($field, $default = null, $force = false)
    {
        $this->parseOptions();

        $result = $this->connection->value($this, $field);

        return false !== $result ? $result : $default;
    }

    /**
     * 得到某个列的数组
     * @access public
     * @param string $field 字段名 多个字段用逗号分隔
     * @param string $key 索引
     * @return array
     */
    public function column($field, $key = '')
    {
        $this->parseOptions();

        return $this->connection->column($this, $field, $key);
    }

    /**
     * 执行command
     * @access public
     * @param string|array|object   $command 指令
     * @param mixed                 $extra 额外参数
     * @param string                $db 数据库名
     * @return array
     */
    public function cmd($command, $extra = null, $db = null)
    {
        return $this->connection->cmd($this, $command, $extra, $db);
    }

    /**
     * 指定distinct查询
     * @access public
     * @param string $field 字段名
     * @return array
     */
    public function distinct($field)
    {
        $result = $this->cmd('distinct', $field);
        return $result[0]['values'];
    }

    /**
     * 获取数据库的所有collection
     * @access public
     * @param string  $db 数据库名称 留空为当前数据库
     * @throws Exception
     */
    public function listCollections($db = '')
    {
        $cursor = $this->cmd('listCollections', null, $db);
        $result = [];
        foreach ($cursor as $collection) {
            $result[] = $collection['name'];
        }
        return $result;
    }

    /**
     * COUNT查询
     * @access public
     * @return integer
     */
    public function count($field = null)
    {
        $this->parseOptions();

        $result = $this->cmd('count');

        return $result[0]['n'];
    }

    /**
     * 聚合查询
     * @access public
     * @param string $aggregate 聚合指令
     * @param string $field     字段名
     * @return mixed
     */
    public function aggregate($aggregate, $field)
    {
        $this->parseOptions();

        $result = $this->cmd('aggregate', [$aggregate, $field]);

        return isset($result[0]['result'][0]['aggregate']) ? $result[0]['result'][0]['aggregate'] : 0;
    }

    /**
     * MAX查询
     * @access public
     * @param string $field   字段名
     * @return float
     */
    public function max($field)
    {
        return $this->aggregate('max', $field);
    }

    /**
     * MIN查询
     * @access public
     * @param string $field   字段名
     * @return mixed
     */
    public function min($field)
    {
        return $this->aggregate('min', $field);
    }

    /**
     * SUM查询
     * @access public
     * @param string $field   字段名
     * @return float
     */
    public function sum($field)
    {
        return $this->aggregate('sum', $field);
    }

    /**
     * AVG查询
     * @access public
     * @param string $field   字段名
     * @return float
     */
    public function avg($field)
    {
        return $this->aggregate('avg', $field);
    }

    /**
     * 字段值(延迟)增长
     * @access public
     * @param string    $field 字段名
     * @param integer   $step 增长值
     * @param integer   $lazyTime 延时时间(s)
     * @return integer|true
     * @throws Exception
     */
    public function setInc($field, $step = 1, $lazyTime = 0)
    {
        $condition = !empty($this->options['where']) ? $this->options['where'] : [];
        if (empty($condition)) {
            // 没有条件不做任何更新
            throw new Exception('no data to update');
        }
        if ($lazyTime > 0) {
            // 延迟写入
            $guid = md5($this->getTable() . '_' . $field . '_' . serialize($condition));
            $step = $this->lazyWrite($guid, $step, $lazyTime);
            if (empty($step)) {
                return true; // 等待下次写入
            }
        }
        return $this->setField($field, ['$inc', $step]);
    }

    /**
     * 字段值（延迟）减少
     * @access public
     * @param string    $field 字段名
     * @param integer   $step 减少值
     * @param integer   $lazyTime 延时时间(s)
     * @return integer|true
     * @throws Exception
     */
    public function setDec($field, $step = 1, $lazyTime = 0)
    {
        $condition = !empty($this->options['where']) ? $this->options['where'] : [];
        if (empty($condition)) {
            // 没有条件不做任何更新
            throw new Exception('no data to update');
        }
        if ($lazyTime > 0) {
            // 延迟写入
            $guid = md5($this->getTable() . '_' . $field . '_' . serialize($condition));
            $step = $this->lazyWrite($guid, -$step, $lazyTime);
            if (empty($step)) {
                return true; // 等待下次写入
            }
        }
        return $this->setField($field, ['$inc', -1 * $step]);
    }

    /**
     * 设置数据
     * @access public
     * @param mixed $field 字段名或者数据
     * @param mixed $value 字段值
     * @return $this
     */
    public function data($field, $value = null)
    {
        if (is_array($field)) {
            $this->options['data'] = isset($this->options['data']) ? array_merge($this->options['data'], $field) : $field;
        } else {
            $this->options['data'][$field] = $value;
        }
        return $this;
    }

    /**
     * 字段值增长
     * @access public
     * @param string|array $field 字段名
     * @param integer      $step  增长值
     * @return $this
     */
    public function inc($field, $step = 1)
    {
        $fields = is_string($field) ? explode(',', $field) : $field;
        foreach ($fields as $field) {
            $this->data($field, ['$inc', $step]);
        }
        return $this;
    }

    /**
     * 字段值减少
     * @access public
     * @param string|array $field 字段名
     * @param integer      $step  减少值
     * @return $this
     */
    public function dec($field, $step = 1)
    {
        $fields = is_string($field) ? explode(',', $field) : $field;
        foreach ($fields as $field) {
            $this->data($field, ['$inc', -1 * $step]);
        }
        return $this;
    }

    /**
     * 分析查询表达式
     * @access public
     * @param string                $logic 查询逻辑    and or xor
     * @param string|array|\Closure $field 查询字段
     * @param mixed                 $op 查询表达式
     * @param mixed                 $condition 查询条件
     * @param array                 $param 查询参数
     * @return void
     */
    protected function parseWhereExp($logic, $field, $op, $condition, $param = [])
    {
        $logic = '$' . strtolower($logic);
        if ($field instanceof \Closure) {
            $this->options['where'][$logic][] = is_string($op) ? [$op, $field] : $field;
            return;
        }
        $where = [];
        if (is_null($op) && is_null($condition)) {
            if (is_array($field)) {
                // 数组批量查询
                $where = $field;
            } elseif ($field) {
                // 字符串查询
                $where[] = ['exp', $field];
            } else {
                $where = '';
            }
        } elseif (is_array($op)) {
            $where[$field] = $param;
        } elseif (in_array(strtolower($op), ['null', 'notnull', 'not null'])) {
            // null查询
            $where[$field] = [$op, ''];
        } elseif (is_null($condition)) {
            // 字段相等查询
            $where[$field] = ['=', $op];
        } else {
            $where[$field] = [$op, $condition];
        }

        if (!empty($where)) {
            if (!isset($this->options['where'][$logic])) {
                $this->options['where'][$logic] = [];
            }
            $this->options['where'][$logic] = array_merge($this->options['where'][$logic], $where);
        }
    }

    /**
     * 指定当前操作的collection
     * @access public
     * @param string $collection
     * @return $this
     */
    public function collection($collection)
    {
        return $this->table($collection);
    }

    /**
     * 不主动获取数据集
     * @access public
     * @param bool $cursor 是否返回 Cursor 对象
     * @return $this
     */
    public function fetchCursor($cursor = true)
    {
        $this->options['fetch_cursor'] = $cursor;
        return $this;
    }

    /**
     * 设置typeMap
     * @access public
     * @param string|array $typeMap
     * @return $this
     */
    public function typeMap($typeMap)
    {
        $this->options['typeMap'] = $typeMap;
        return $this;
    }

    /**
     * awaitData
     * @access public
     * @param bool $awaitData
     * @return $this
     */
    public function awaitData($awaitData)
    {
        $this->options['awaitData'] = $awaitData;
        return $this;
    }

    /**
     * batchSize
     * @access public
     * @param integer $batchSize
     * @return $this
     */
    public function batchSize($batchSize)
    {
        $this->options['batchSize'] = $batchSize;
        return $this;
    }

    /**
     * exhaust
     * @access public
     * @param bool $exhaust
     * @return $this
     */
    public function exhaust($exhaust)
    {
        $this->options['exhaust'] = $exhaust;
        return $this;
    }

    /**
     * 设置modifiers
     * @access public
     * @param array $modifiers
     * @return $this
     */
    public function modifiers($modifiers)
    {
        $this->options['modifiers'] = $modifiers;
        return $this;
    }

    /**
     * 设置noCursorTimeout
     * @access public
     * @param bool $noCursorTimeout
     * @return $this
     */
    public function noCursorTimeout($noCursorTimeout)
    {
        $this->options['noCursorTimeout'] = $noCursorTimeout;
        return $this;
    }

    /**
     * 设置oplogReplay
     * @access public
     * @param bool $oplogReplay
     * @return $this
     */
    public function oplogReplay($oplogReplay)
    {
        $this->options['oplogReplay'] = $oplogReplay;
        return $this;
    }

    /**
     * 设置partial
     * @access public
     * @param bool $partial
     * @return $this
     */
    public function partial($partial)
    {
        $this->options['partial'] = $partial;
        return $this;
    }

    /**
     * maxTimeMS
     * @access public
     * @param string $maxTimeMS
     * @return $this
     */
    public function maxTimeMS($maxTimeMS)
    {
        $this->options['maxTimeMS'] = $maxTimeMS;
        return $this;
    }

    /**
     * collation
     * @access public
     * @param array $collation
     * @return $this
     */
    public function collation($collation)
    {
        $this->options['collation'] = $collation;
        return $this;
    }

    /**
     * 设置返回字段
     * @access public
     * @param array     $field
     * @param boolean   $except 是否排除
     * @return $this
     */
    public function field($field, $except = false, $tableName = '', $prefix = '', $alias = '')
    {
        if (is_string($field)) {
            $field = array_map('trim', explode(',', $field));
        }

        $projection = [];
        foreach ($field as $key => $val) {
            if (is_numeric($key)) {
                $projection[$val] = $except ? 0 : 1;
            } else {
                $projection[$key] = $val;
            }
        }

        $this->options['projection'] = $projection;

        return $this;
    }

    /**
     * 设置skip
     * @access public
     * @param integer $skip
     * @return $this
     */
    public function skip($skip)
    {
        $this->options['skip'] = $skip;
        return $this;
    }

    /**
     * 设置slaveOk
     * @access public
     * @param bool $slaveOk
     * @return $this
     */
    public function slaveOk($slaveOk)
    {
        $this->options['slaveOk'] = $slaveOk;
        return $this;
    }

    /**
     * 指定查询数量
     * @access public
     * @param mixed $offset 起始位置
     * @param mixed $length 查询数量
     * @return $this
     */
    public function limit($offset, $length = null)
    {
        if (is_null($length)) {
            if (is_numeric($offset)) {
                $length = $offset;
                $offset = 0;
            } else {
                list($offset, $length) = explode(',', $offset);
            }
        }
        $this->options['skip']  = intval($offset);
        $this->options['limit'] = intval($length);

        return $this;
    }

    /**
     * 设置sort
     * @access public
     * @param array|string|object   $field
     * @param string                $order
     * @return $this
     */
    public function order($field, $order = '')
    {
        if (is_array($field)) {
            $this->options['sort'] = $field;
        } else {
            $this->options['sort'][$field] = 'asc' == strtolower($order) ? 1 : -1;
        }
        return $this;
    }

    /**
     * 设置tailable
     * @access public
     * @param bool $tailable
     * @return $this
     */
    public function tailable($tailable)
    {
        $this->options['tailable'] = $tailable;
        return $this;
    }

    /**
     * 设置writeConcern对象
     * @access public
     * @param WriteConcern $writeConcern
     * @return $this
     */
    public function writeConcern($writeConcern)
    {
        $this->options['writeConcern'] = $writeConcern;
        return $this;
    }

    /**
     * 把主键值转换为查询条件 支持复合主键
     * @access public
     * @param array|string  $data 主键数据
     * @param mixed         $options 表达式参数
     * @return void
     * @throws Exception
     */
    public function parsePkWhere($data)
    {
        $pk = $this->getPk($this->options);

        if (is_string($pk)) {
            // 根据主键查询
            if (is_array($data)) {
                $where[$pk] = isset($data[$pk]) ? $data[$pk] : ['in', $data];
            } else {
                $where[$pk] = strpos($data, ',') ? ['in', $data] : $data;
            }
        }

        if (!empty($where)) {
            if (isset($this->options['where']['$and'])) {
                $this->options['where']['$and'] = array_merge($this->options['where']['$and'], $where);
            } else {
                $this->options['where']['$and'] = $where;
            }
        }

        return;
    }

    /**
     * 插入记录
     * @access public
     * @param mixed     $data 数据
     * @param boolean   $replace      是否replace（目前无效）
     * @param boolean   $getLastInsID 返回自增主键
     * @return WriteResult
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function insert(array $data = [], $replace = null, $getLastInsID = false, $sequence = null)
    {
        $this->parseOptions();

        $this->options['data'] = array_merge($this->options['data'], $data);

        return $this->connection->insert($this, $replace, $getLastInsID);
    }

    /**
     * 插入记录并获取自增ID
     * @access public
     * @param mixed $data 数据
     * @return integer
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function insertGetId(array $data = [], $replace = false, $sequence = null)
    {
        return $this->insert($data, null, true);
    }

    /**
     * 批量插入记录
     * @access public
     * @param mixed $dataSet 数据集
     * @return integer
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function insertAll(array $dataSet)
    {
        $this->parseOptions();

        return $this->connection->insertAll($this, $dataSet);
    }

    /**
     * 更新记录
     * @access public
     * @param mixed $data 数据
     * @return int
     * @throws Exception
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function update(array $data = [])
    {
        $this->parseOptions();

        $this->options['data'] = array_merge($this->options['data'], $data);

        return $this->connection->update($this);
    }

    /**
     * 删除记录
     * @access public
     * @param array $data 表达式 true 表示强制删除
     * @return int
     * @throws Exception
     * @throws AuthenticationException
     * @throws InvalidArgumentException
     * @throws ConnectionException
     * @throws RuntimeException
     * @throws BulkWriteException
     */
    public function delete($data = null)
    {
        $this->parseOptions();

        $this->options['data'] = $data;

        return $this->connection->delete($this);
    }

    /**
     * 执行查询但只返回Cursor对象
     * @access public
     * @return Cursor
     */
    public function getCursor()
    {
        $this->parseOptions();

        return $this->connection->getCursor($this);
    }

    /**
     * 查询数据转换为模型对象
     * @access public
     * @param array $result     查询数据
     * @param array $options    查询参数
     * @param bool  $resultSet  是否为数据集查询
     * @return void
     */
    protected function resultToModel(&$result, $options = [], $resultSet = false)
    {

        $condition = (!$resultSet && isset($options['where']['$and'])) ? $options['where']['$and'] : null;
        $result    = $this->model->newInstance($result, $condition);

        // 关联查询
        if (!empty($options['relation'])) {
            $result->relationQuery($options['relation']);
        }

        // 预载入查询
        if (!$resultSet && !empty($options['with'])) {
            $result->eagerlyResult($result, $options['with']);
        }

        // 关联统计
        if (!empty($options['with_count'])) {
            $result->relationCount($result, $options['with_count']);
        }

    }

    /**
     * 分批数据返回处理
     * @access public
     * @param integer   $count 每次处理的数据数量
     * @param callable  $callback 处理回调方法
     * @param string    $column 分批处理的字段名
     * @return boolean
     */
    public function chunk($count, $callback, $column = null)
    {
        $column    = $column ?: $this->getPk();
        $options   = $this->getOptions();
        $resultSet = $this->limit($count)->order($column, 'asc')->select();

        while (!empty($resultSet)) {
            if (false === call_user_func($callback, $resultSet)) {
                return false;
            }
            $end       = end($resultSet);
            $lastId    = is_array($end) ? $end[$column] : $end->$column;
            $resultSet = $this->options($options)
                ->limit($count)
                ->where($column, '>', $lastId)
                ->order($column, 'asc')
                ->select();
        }
        return true;
    }

    /**
     * 分析表达式（可用于查询或者写入操作）
     * @access protected
     * @return array
     */
    protected function parseOptions()
    {
        $options = $this->options;

        // 获取数据表
        if (empty($options['table'])) {
            $options['table'] = $this->getTable();
        }

        foreach (['where', 'data'] as $name) {
            if (!isset($options[$name])) {
                $options[$name] = [];
            }
        }

        $modifiers = empty($options['modifiers']) ? [] : $options['modifiers'];
        if (isset($options['comment'])) {
            $modifiers['$comment'] = $options['comment'];
        }

        if (isset($options['maxTimeMS'])) {
            $modifiers['$maxTimeMS'] = $options['maxTimeMS'];
        }

        if (!empty($modifiers)) {
            $options['modifiers'] = $modifiers;
        }

        if (!isset($options['projection']) || '*' == $options['projection']) {
            $options['projection'] = [];
        }

        if (!isset($options['typeMap'])) {
            $options['typeMap'] = $this->getConfig('type_map');
        }

        if (!isset($options['limit'])) {
            $options['limit'] = 0;
        }

        foreach (['master', 'fetch_cursor'] as $name) {
            if (!isset($options[$name])) {
                $options[$name] = false;
            }
        }

        if (isset($options['page'])) {
            // 根据页数计算limit
            list($page, $listRows) = $options['page'];
            $page                  = $page > 0 ? $page : 1;
            $listRows              = $listRows > 0 ? $listRows : (is_numeric($options['limit']) ? $options['limit'] : 20);
            $offset                = $listRows * ($page - 1);
            $options['skip']       = intval($offset);
            $options['limit']      = intval($listRows);
        }

        $this->options = $options;

        return $options;
    }

}
