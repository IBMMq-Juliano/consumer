package com.example;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQGetMessageOptions;

/**
 * IBMConsumer - Classe responsável por consumir mensagens de uma fila no IBM
 * MQ.
 *
 * Esta classe estabelece conexão com um gerenciador de filas do IBM MQ e lê
 * mensagens de uma fila específica em um loop contínuo, processando-as à medida que chegam.
 * 
 * @author Juliano Souza <juliano.souza@scalait.com>
 * @version 1.0
 * @since 2024
 */
public class IBMConsumer {

    /** Nome do Gerenciador de Filas */
    private static final String QMGR_EXAMPLES = "QMEXAMPLES";

    /** Nome do canal de comunicação */
    private static final String CHANNEL_EXAMPLES = "ADMIN.CHL";

    /** Nome do host do IBM MQ */
    private static final String CONN_NAME_EXAMPLES_HOSTNAME = "localhost";

    /** Porta de conexão do IBM MQ */
    private static final String CONN_NAME_EXAMPLES_PORT = "1616";

    /**
     * Método principal para iniciar o consumidor.
     *
     * @param args Nome da fila a ser lida como argumento.
     */
    public static void main(String[] args) {

        // Verifica se o nome da fila foi passado como argumento
        if (args.length < 1) {
            System.err.println("Erro: É necessário informar o nome da fila");
            return;
        }

        // Nome da fila a ser lida
        String queueName = args[0];

        MQQueueManager queueManager = null;
        MQQueue queue = null;

        try {
            // Configuração do ambiente MQ
            com.ibm.mq.MQEnvironment.hostname = CONN_NAME_EXAMPLES_HOSTNAME;
            com.ibm.mq.MQEnvironment.port = Integer.parseInt(CONN_NAME_EXAMPLES_PORT);
            com.ibm.mq.MQEnvironment.channel = CHANNEL_EXAMPLES;

            // Conexão ao Gerenciador de Filas
            queueManager = new MQQueueManager(QMGR_EXAMPLES);

            // Opções para acessar a fila
            int openOptions = CMQC.MQOO_INPUT_AS_Q_DEF | CMQC.MQOO_FAIL_IF_QUIESCING;

            // Acesso à fila especificada
            queue = queueManager.accessQueue(queueName, openOptions);

            // Loop infinito para esperar e processar mensagens
            while (true) {
                MQMessage message = new MQMessage();
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                gmo.options = openOptions;
                gmo.waitInterval = -1; // Espera indefinida por uma nova mensagem

                // Lê uma mensagem da fila
                queue.get(message, gmo);

                // Lê e exibe o conteúdo da mensagem como uma string
                String messageText = message.readStringOfByteLength(message.getDataLength());
                System.out.println(messageText);
            }
        } catch (MQException e) {
            System.err.println("Erro no MQ: " + e.getMessage() + ", Código de Erro: " + e.getReason());
        } catch (Exception e) {
            System.err.println("Erro geral: " + e.getMessage());
        } finally {
            try {
                // Fechando a fila e desconectando do Gerenciador de Filas
                if (queue != null)
                    queue.close();
                if (queueManager != null)
                    queueManager.disconnect();
            } catch (MQException e) {
                System.err.println("Erro ao fechar recursos: " + e.getMessage());
            }
        }
    }
}
