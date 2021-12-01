<?php

namespace Firebird\Query;

use Illuminate\Database\Query\Builder as QueryBuilder;

class Builder extends QueryBuilder
{
    /**
     * Add a from stored procedure clause to the query builder.
     *
     * @param string $procedure
     * @param array $values
     * @return \Illuminate\Database\Query\Builder|static
     */
    public function fromProcedure(string $procedure, array $values = [])
    {
        $compiledProcedure = $this->grammar->compileProcedure($this, $procedure, $values);

        $this->fromRaw($compiledProcedure, array_values($values));

        return $this;
    }


    /**
     * Get context variable value
     *
     * @param string $namespace
     * @param string $name
     * @return mixed
     */
    public function getContextValue($namespace, $name)
    {
        $sql = $this->grammar->compileGetContext($this, $namespace, $name);

        return $this->processor->processGetContextValue($this, $sql);
    }

    /**
     * Get next sequence value
     *
     * @param string $sequence
     * @param int $increment
     * @return int
     */
    public function nextSequenceValue($sequence = null, $increment = null)
    {
        $sql = $this->grammar->compileNextSequenceValue($this, $sequence, $increment);

        return $this->processor->processNextSequenceValue($this, $sql);
    }

    /**
     * Execute stored procedure
     *
     * @param string $procedure
     * @param array $values
     */
    public function executeProcedure($procedure, array $values = null)
    {
        if (!$values) {
            $values = [];
        }

        $bindings = array_values($values);

        $sql = $this->grammar->compileExecProcedure($this, $procedure, $values);

        $this->connection->statement($sql, $this->cleanBindings($bindings));
    }

    /**
     * Execute stored function
     *
     * @param string $function
     * @param array $values
     *
     * @return mixed
     */
    public function executeFunction($function, array $values = null)
    {
        if (!$values) {
            $values = [];
        }

        $sql = $this->grammar->compileExecProcedure($this, $function, $values);

        return $this->processor->processExecuteFunction($this, $sql, $values);
    }
}
