<?php // vim:set ts=4 sw=4 et:

namespace ElasticSearch\Transport;

use \ElasticSearch\DSL\Stringify;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * This file is part of the ElasticSearch PHP client
 *
 * (c) Dan Krieger <dan@navigationnorth.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

class RabbitMQ extends Base {

    protected $user, $past, $vhost, $connParams;

    public function __construct($host="127.0.0.1", $port=5672, $user = 'guest', $pass = 'guest', $vhost = '/') {
        parent::__construct($host, $port);

        $this->user = $user;
        $this->pass = $pass;
        $this->vhost = $vhost;

        $this->connParams = array(
            'host' => $host,
            'port' => $port,
            'username' => $user,
            'password' => $pass,
            'vhost' => $vhost
        );
    }

    /**
     * Index a new document or update it if existing
     *
     * @return array
     * @param array $document
     * @param mixed $id Optional
     * @param array $options
     * @throws \ElasticSearch\Exception
     */
    public function index($document, $id=false, array $options = array()) {
        if ($id === false)
            throw new \ElasticSearch\Exception("RabbitMQ transport requires id when indexing");

        $meta = array(
            '_index' => isset($options['_index']) ? $options['_index'] : $this->index,
            '_type' => isset($options['_type']) ? $options['_type'] : $this->type,
            '_id' => $id
        );

        return $this->sendRequest(array('index' => $meta), $document);
    }

    /**
     * Search
     *
     * @return array
     * @param array|string $query
     * @throws \ElasticSearch\Exception
     */
    public function search($query) {
        throw new \ElasticSearch\Exception("RabbitMQ protocol doesnt support search");
    }

    /**
     * Perform a request against the given path/method/payload combination
     * Example:
     * $es->request('/_status');
     *
     * It does only support GET and DELETE requests and silently ignores the
     * payload.
     *
     * @param string|array $path
     * @param string $method
     * @param array|string|bool $payload
     * @return array
     */
    public function request($path, $method="GET", $payload=false) {
        switch(head($path))
        {
            case '/_bulk':
                return $this->sendRequest($payload);
            default:
                throw new Exception("RabbitMQ does not support $method-requests.");
        }
    }

    /**
     * Flush this index/type combination
     *
     * @return array
     * @param mixed $id
     * @param array $options Parameters to pass to delete action
     */
    public function delete($id=false, array $options = array()) {
        if (!$id)
            throw new Exception("RabbitMQ implementation requires id for DELETE");

        $meta = array(
            '_index' => isset($options['_index']) ? $options['_index'] : $this->index,
            '_type' => isset($options['_type']) ? $options['_type'] : $this->type,
            '_id' => $id
        );

        return $this->sendRequest(array('delete' => $meta));
    }


    public function sendRequest($meta, $doc = null)
    {
        $message = is_string($meta) ? $meta : json_encode($meta);

        if($doc)
        {
            $message .= "\n".json_encode($doc);
        }

        //Assure we have a new line at the end of our final line so that bulk parser reads it properly
        if($message[strlen($message) -1] != "\n")
        {
           $message .= "\n";
        }

        $exchange = 'elasticsearch';
        $routingKey = 'elasticsearch';

        $conn = $this->_openConnection();

        $channel = $conn->channel();

        $channel->exchange_declare($exchange, 'direct', false, true, false);

        $msg = new AMQPMessage($message, array('content_type' => 'text/plain', 'content_encoding' => 'UTF-8'));

        $result = array('result' => $channel->basic_publish($msg, $exchange, $routingKey));

        $channel->close();

        $conn->close();

        return $result;
    }

    protected function _openConnection()
    {
        return new AMQPConnection(
            $this->connParams['host'],
            $this->connParams['port'],
            $this->connParams['username'],
            $this->connParams['password'],
            $this->connParams['vhost']
        );
    }
}
