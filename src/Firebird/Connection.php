<?php

namespace Firebird;

use Exception;
use Firebird\Query\Builder as FirebirdQueryBuilder;
use Firebird\Query\Grammars\Firebird1Grammar as Firebird1QueryGrammar;
use Firebird\Query\Grammars\Firebird2Grammar as Firebird2QueryGrammar;
use Firebird\Query\Grammars\Firebird30Grammar;
use Firebird\Query\Processors\FirebirdProcessor;
use Firebird\Schema\Builder as FirebirdSchemaBuilder;
use Firebird\Schema\Grammars\FirebirdGrammar as FirebirdSchemaGrammar;
use Firebird\Support\Version;
use Illuminate\Database\Connection as DatabaseConnection;

class Connection extends DatabaseConnection
{

    /**
     * Firebird Engine version
     *
     * @var string
     */
    private $engine_version = null;


    /**
     * Get engine version
     *
     * @return string
     */
    protected function getEngineVersion()
    {
        if (!$this->engine_version) {
            $this->engine_version = isset($this->config['engine_version']) ? $this->config['engine_version'] : null;
        }
        if (!$this->engine_version) {
            $sql = "SELECT RDB\$GET_CONTEXT(?, ?) FROM RDB\$DATABASE";
            $sth = $this->getPdo()->prepare($sql);
            $sth->execute(['SYSTEM', 'ENGINE_VERSION']);
            $this->engine_version = $sth->fetchColumn();
            $sth->closeCursor();
        }
        return $this->engine_version;
    }

    /**
     * Get major engine version
     * It allows you to determine the features of the engine.
     *
     * @return int
     */
    protected function getMajorEngineVersion()
    {
        $version = $this->getEngineVersion();
        $parts = explode('.', $version);
        return (int)$parts[0];
    }



    /**
     * Get the default query grammar instance.
     *
     * @return \Firebird\Query\Grammars\FirebirdGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        // if ($this->getFirebirdVersion() == Version::FIREBIRD_15) {
        //     return new Firebird1QueryGrammar;
        // }

        // return new Firebird2QueryGrammar;
        if ($this->getFirebirdVersion() == Version::FIREBIRD_30) {
            return new Firebird30Grammar;
        }

        return new Exception("The Firebird version provided is not supported.");
    }

    /**
     * Get the default post processor instance.
     *
     * @return \Firebird\Query\Processors\FirebirdProcessor
     */
    protected function getDefaultPostProcessor()
    {
        return new FirebirdProcessor;
    }

    /**
     * Get a schema builder instance for this connection.
     *
     * @return \Firebird\Schema\Builder
     */
    public function getSchemaBuilder()
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new FirebirdSchemaBuilder($this);
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return \Firebird\Schema\Grammars\FirebirdGrammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new FirebirdSchemaGrammar);
    }

    /**
     * Get query builder.
     *
     * @return \Firebird\Query\Builder
     */
    protected function getQueryBuilder()
    {
        return new FirebirdQueryBuilder(
            $this,
            $this->getQueryGrammar(),
            $this->getPostProcessor()
        );
    }

    /**
     * Get a new query builder instance.
     *
     * @return \Firebird\Query\Builder
     */
    public function query()
    {
        return $this->getQueryBuilder();
    }

    /**
     * Execute stored function
     *
     * @param string $function
     * @param array $values
     * @return mixed
     */
    public function executeFunction($function, array $values = null)
    {
        $query = $this->getQueryBuilder();

        return $query->executeFunction($function, $values);
    }

    /**
     * Execute a stored procedure.
     *
     * @param string $procedure
     * @param array $values
     *
     * @return \Illuminate\Support\Collection
     */
    public function executeProcedure($procedure, array $values = [])
    {
        return $this->query()->fromProcedure($procedure, $values)->get();
    }

    /**
     * The Firebird database version that should be used when compiling queries.
     *
     * @return string
     */
    protected function getFirebirdVersion()
    {
        if (!array_key_exists('version', $this->config)) {
            return Version::FIREBIRD_30;
        }

        // Check the user has provided a supported version.
        if (!in_array($this->config['version'], Version::SUPPORTED_VERSIONS)) {
            throw new Exception('The Firebird version provided is not supported.');
        }

        return $this->config['version'];
    }

    /**
     * Start a new database transaction.
     *
     * @return void
     * @throws \Exception
     */
    public function beginTransaction()
    {
        if ($this->transactions == 0 && $this->pdo->getAttribute(PDO::ATTR_AUTOCOMMIT) == 1) {
            $this->pdo->setAttribute(PDO::ATTR_AUTOCOMMIT, 0);
        }
        parent::beginTransaction();
    }

    /**
     * Commit the active database transaction.
     *
     * @return void
     */
    public function commit()
    {
        parent::commit();
        if ($this->transactions == 0 && $this->pdo->getAttribute(PDO::ATTR_AUTOCOMMIT) == 0) {
            $this->pdo->setAttribute(PDO::ATTR_AUTOCOMMIT, 1);
        }
    }

    /**
     * Rollback the active database transaction.
     *
     * @param int|null $toLevel
     * @return void
     * @throws \Exception
     */
    public function rollBack($toLevel = null)
    {
        parent::rollBack($toLevel);
        if ($this->transactions == 0 && $this->pdo->getAttribute(PDO::ATTR_AUTOCOMMIT) == 0) {
            $this->pdo->setAttribute(PDO::ATTR_AUTOCOMMIT, 1);
        }
    }
}
