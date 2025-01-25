#include <iostream>     /* cerr */
#include <algorithm>
#include "supservidor.h"

using namespace std;

/* ========================================
   CLASSE SUPSERVIDOR
   ======================================== */

/// Construtor
SupServidor::SupServidor()
  : Tanks()
  , server_on(false)
  , LU()
  , MyServerThread()
  , MyServerSocket()
{
  // Inicializa a biblioteca de sockets
  mysocket_status initResult = mysocket::init();
  // Em caso de erro, mensagem e encerra
  if (initResult == mysocket_status::SOCK_ERROR)
  {
    cerr <<  "Biblioteca mysocket nao pode ser inicializada";
    exit(-1);
  }
  
  /// End of SupServidor class
}


/// Destrutor
SupServidor::~SupServidor()
{
  // Deve parar a thread do servidor
  server_on = false;

  // Fecha todos os sockets dos clientes
  for (auto& U : LU) U.close();
  
  // Fecha o socket de conexoes
  MyServerSocket.close();

  // Espera o fim da thread do servidor
  if(MyServerThread.joinable())
  {
      MyServerThread.join();
  }

  // Encerra a biblioteca de sockets
  mysocket::end();
}

/// Liga o servidor
bool SupServidor::setServerOn()
{
  // Se jah estah ligado, nao faz nada
  if (server_on) return true;

  // Liga os tanques
  setTanksOn();

  // Indica que o servidor estah ligado a partir de agora
  server_on = true;

  try
  {
    // Coloca o socket de conexoes em escuta
    mysocket_status listenResult = MyServerSocket.listen( SUP_PORT, LU.size());
    // Em caso de erro, gera excecao
    if ( listenResult == mysocket_status::SOCK_ERROR ) throw 1;

    // Lanca a thread do servidor que comunica com os clientes
    MyServerThread = thread(&SupServidor::thr_server_main, this);
    // Em caso de erro, gera excecao
    if (!MyServerThread.joinable()) throw 2;
  }
  catch(int i)
  {
    cerr << "Erro " << i << " ao iniciar o servidor\n";

    // Deve parar a thread do servidor
    server_on = false;

    // Fecha o socket do servidor
    MyServerSocket.close();

    return false;
  }

  // Tudo OK
  return true;
}

/// Desliga o servidor
void SupServidor::setServerOff()
{
  // Se jah estah desligado, nao faz nada
  if (!server_on) return;

  // Deve parar a thread do servidor
  server_on = false;

  // Fecha todos os sockets dos clientes
  for (auto& U : LU) U.close();
  // Fecha o socket de conexoes
  MyServerSocket.close();

  // Espera pelo fim da thread do servidor
  if(MyServerThread.joinable())
  {
      MyServerThread.join();
  }
  // Faz o identificador da thread apontar para thread vazia
  MyServerThread = thread();

  // Desliga os tanques
  setTanksOff();
}

/// Leitura do estado dos tanques
void SupServidor::readStateFromSensors(SupState& S) const
{
  // Estados das valvulas: OPEN, CLOSED
  S.V1 = v1isOpen();
  S.V2 = v2isOpen();
  // Niveis dos tanques: 0 a 65535
  S.H1 = hTank1();
  S.H2 = hTank2();
  // Entrada da bomba: 0 a 65535
  S.PumpInput = pumpInput();
  // Vazao da bomba: 0 a 65535
  S.PumpFlow = pumpFlow();
  // Estah transbordando (true) ou nao (false)
  S.ovfl = isOverflowing();
}

/// Leitura e impressao em console do estado da planta
void SupServidor::readPrintState() const
{
  if (tanksOn())
  {
    SupState S;
    readStateFromSensors(S);
    S.print();
  }
  else
  {
    cout << "Tanques estao desligados!\n";
  }
}

/// Impressao em console dos usuarios do servidor
void SupServidor::printUsers() const
{
  for (const auto& U : LU)
  {
    cout << U.login << '\t'
         << "Admin=" << (U.isAdmin ? "SIM" : "NAO") << '\t'
         << "Conect=" << (U.isConnected() ? "SIM" : "NAO") << '\n';
  }
}

/// Adicionar um novo usuario
bool SupServidor::addUser(const string& Login, const string& Senha,
                             bool Admin)
{
  // Testa os dados do novo usuario
  if (Login.size()<6 || Login.size()>12) return false;
  if (Senha.size()<6 || Senha.size()>12) return false;

  // Testa se jah existe usuario com mesmo login
  auto itr = find(LU.begin(), LU.end(), Login);
  if (itr != LU.end()) return false;

  // Insere
  LU.push_back( User(Login,Senha,Admin) );

  // Insercao OK
  return true;
}

/// Remover um usuario
bool SupServidor::removeUser(const string& Login)
{
  // Testa se existe usuario com esse login
  auto itr = find(LU.begin(), LU.end(), Login);
  if (itr == LU.end()) return false;

  // Remove
  LU.erase(itr);

  // Remocao OK
  return true;
}

/// A thread que implementa o servidor.
/// Comunicacao com os clientes atraves dos sockets.
void SupServidor::thr_server_main(void)
{
	//variaveis auxiliares
	// Fila de sockets para aguardar chegada de dados
	mysocket_queue fila_sockets;
	//armazena os bytes do comando
	uint16_t cmd;
	//armazena o resultado do retorno das funcoes dos sockets
	mysocket_status retorno_socket;
	//verifica o retorno de had activity
	bool teve_atividade;
	//socket temporario
	tcp_mysocket socket_temp;
	//dados da nova conexao
	string login, senha, adm;
          bool admin;


  while (server_on)
  {
    // Erros mais graves que encerram o servidor
    // Parametro do throw e do catch eh uma const char* = "texto"
    try
    {
      // Encerra se o socket de conexoes estiver fechado
      if (MyServerSocket.closed())
      {
        throw "socket de conexoes fechado";
      }

      // Inclui na fila de sockets todos os sockets que eu
      // quero monitorar para ver se houve chegada de dados

      // Limpa a fila de sockets
      fila_sockets.clear();
      
	  // Inclui na fila o socket de conexoes
      fila_sockets.include(MyServerSocket);
      
	  // Inclui na fila todos os sockets dos clientes conectados
      for(auto &cliente: LU)
      {
        if(cliente.isConnected())
        {
          fila_sockets.include(cliente.MyConexion);
        }
      }

      // Espera ateh que chegue dado em algum socket (com timeout)
      mysocket_status wait_read_result = fila_sockets.wait_read(SUP_TIMEOUT);
      // De acordo com o resultado da espera:
      
    	switch (wait_read_result) // resultado do wait_read
		{
			// SOCK_TIMEOUT:
			// Saiu por timeout: nao houve atividade em nenhum socket
			// Aproveita para salvar dados ou entao nao faz nada
			case mysocket_status::SOCK_TIMEOUT:
			break;

			// SOCK_ERROR:
			// Erro no select: encerra o servidor
			case mysocket_status::SOCK_ERROR:
				SupServidor::setServerOff();
			break;
			
			// SOCK_OK:
			// Houve atividade em algum socket da fila:
			//   Testa se houve atividade nos sockets dos clientes. Se sim:
			//   - Leh o comando
			//   - Executa a acao
			//   = Envia resposta
			//   Depois, testa se houve atividade no socket de conexao. Se sim:
			//   - Estabelece nova conexao em socket temporario
			//   - Leh comando, login e senha
			//   - Testa usuario
			//   - Se deu tudo certo, faz o socket temporario ser o novo socket
			//     do cliente e envia confirmacao

			case mysocket_status::SOCK_OK:
				for(auto &cliente:LU)
				{
					if(fila_sockets.had_activity(cliente.MyConexion))
					{
						//leh o comando
						retorno_socket = cliente.MyConexion.read_uint16(cmd);

						//verifica se o comando foi lido corretamente
						if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o comando";

						//executa a acao
						switch(cmd)
						{
							case SupCommands::CMD_LOGIN:// RECEBE COMANDO DE LOGIN DO CLIENTE
							
								//leh o login do usuario
								retorno_socket = cliente.MyConexion.read_string(login, SUP_TIMEOUT);
								if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o login\n";

								//leh a senha do usuario
								retorno_socket = cliente.MyConexion.read_string(senha, SUP_TIMEOUT);
								if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler a senha\n";

								//leh se o usuario eh admin ou nao
								retorno_socket = cliente.MyConexion.read_string(adm, SUP_TIMEOUT);
								if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o status de admin\n";

								//tratando a variável adm para converter para bool
								transform(adm.begin(), adm.end(), adm.begin(), ::toupper);
								
								//verificando se o usuário digitou "S" ou "N" que são as opções válidas para determinar a variável admin
								if(adm=="S") admin = true;
								else admin = false;

								if(addUser(login,senha,admin))
								{
									if(admin) cliente.MyConexion.write_uint16(SupCommands::CMD_ADMIN_OK);
									else cliente.MyConexion.write_uint16(SupCommands::CMD_OK);
								}
								else{
									cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
									throw "Erro ao adicionar o usuário\n";
								}
								break;
							case SupCommands::CMD_GET_DATA://	RECEBE COMANDO GET DATA DO CLIENTE
								 // ESCREVE NO SOQUETE DO CLIENTE AS INFORMAÇÕES DOS TANQUES
								 retorno_socket = cliente.MyConexion.write_uint16(SupServidor::v1isOpen());         // Estado da valvula 1: aberta (!=0) ou fechada (==0)
								 if(retorno_socket != mysocket_status::SOCK_OK) 
								 	cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
									throw "Erro ao escrever o valor da válvula 1\n";
								 
								 
								 retorno_socket = cliente.MyConexion.write_uint16(SupServidor::v2isOpen());         // Estado da valvula 2: aberta (!=0) ou fechada (==0)
								 if(retorno_socket != mysocket_status::SOCK_OK) 
								 	cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
								 	throw "Erro ao escrever o valor da válvula 2\n";
								 
								 
								 retorno_socket = cliente.MyConexion.write_uint16(SupServidor::hTank1());           // Medida do sensor de nivel tanque 1: 0 a 65535
								 if(retorno_socket != mysocket_status::SOCK_OK) 
								 	cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
								 	throw "Erro ao escrever o valor do sensor de nível do tanque 1\n";
								 
								 
								 retorno_socket = cliente.MyConexion.write_uint16(SupServidor::hTank2());           // Medida do sensor de nivel tanque 2: 0 a 65535
								 if(retorno_socket != mysocket_status::SOCK_OK) 
								 	cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
									throw "Erro ao escrever o valor do sensor de nível do tanque 2\n";
								 
								 
								 retorno_socket = cliente.MyConexion.write_uint16(SupServidor::pumpInput());        // Entrada da bomba: 0 a 65535
								 if(retorno_socket != mysocket_status::SOCK_OK)
								 {
								 	cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
								 	throw "Erro ao escrever o valor da bomba\n";
								 }
								 
								 retorno_socket = cliente.MyConexion.write_uint16(SupServidor::pumpFlow());         // Medida do sensor de vazao da bomba: 0 a 65535
								 if(retorno_socket != mysocket_status::SOCK_OK)
								 { 
								 	cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
								 	throw "Erro ao escrever o valor do sensor de vazão da bomba\n";
								 }
								 
								 retorno_socket = cliente.MyConexion.write_uint16(SupServidor::isOverflowing());    // Estah transbordando: sim (!=0) ou nao (==0)
								 if(retorno_socket != mysocket_status::SOCK_OK)
								 { 
								 	cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
								 	throw "Erro ao escrever o valor de transbordamento\n";
								 }
								 
								 retorno_socket = cliente.MyConexion.write_uint16(SupCommands::CMD_OK);
								 if(retorno_socket != mysocket_status::SOCK_OK)
								 {
									 cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);								 
								 	 throw "Erro ao escrever o comando de OK\n";
								 }
								break;
							case SupCommands::CMD_SET_V1://		RECEBE COMANDO SET V1 DO CLIENTE
								//ESCREVE NO SOQUETE DO CLIENTE A CONFIRMAÇÃO DE QUE A VÁLVULA 1 FOI ALTERADA OU NÃO
								
								//VERIFICA SE O CLIENTE PODE EXECUTAR ESSE COMANDO
								if(cliente.isAdmin)
								{
									//armazena o valor inserido pelo cliente
									uint16_t v1;
									retorno_socket= cliente.MyConexion.read_uint16(v1, SUP_TIMEOUT);
									if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o valor do usuário para alterar a válvula 1\n";
									
									if(v1==0) SupServidor::setV1Open(false);
									else SupServidor::setV1Open(true);
									//A VÁLVULA V1 FOI ALTERADA COM SUCESSO
									cliente.MyConexion.write_uint16(SupCommands::CMD_OK);
								}
								else
						 		{
									cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
									throw "Erro: usuário não é administrador\n";//	ERRO NA ALTERAÇÃO DO VALOR DA VÁLVULA V1. USUARIO NÃO TEM PERMISSÃO
								}
								break;
							case SupCommands::CMD_SET_V2:// 	RECEBE COMANDO SET V2 DO CLIENTE
								//ESCREVE NO SOQUETE DO CLIENTE A CONFIRMAÇÃO DE QUE A VÁLVULA 1 FOI ALTERADA OU NÃO
								
								//VERIFICA SE O CLIENTE PODE EXECUTAR ESSE COMANDO
								if(cliente.isAdmin)
								{
									//armazena o valor inserido pelo cliente
									uint16_t v2;
									retorno_socket= cliente.MyConexion.read_uint16(v2, SUP_TIMEOUT);
									if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o valor do usuário para alterar a válvula 1\n";
									
									if(v2==0) SupServidor::setV2Open(false);
									else SupServidor::setV2Open(true);
									//A VÁLVULA V2 FOI ALTERADA COM SUCESSO
									cliente.MyConexion.write_uint16(SupCommands::CMD_OK);
								}
								else
								{
									cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
									throw "Erro: usuário não é administrador\n";//	ERRO NA ALTERAÇÃO DO VALOR DA VÁLVULA V2. USUARIO NÃO TEM PERMISSÃO							
								} 
								break;
							case SupCommands::CMD_SET_PUMP://	RECEBE COMANDO SET PUMP DO CLIENTE
								//ESCREVE NO SOQUETE DO CLIENTE A CONFIRMAÇÃO DE QUE A BOMBA FOI ALTERADA OU NÃO

								//VERIFICA SE O CLIENTE PODE EXECUTAR ESSE COMANDO
								if(cliente.isAdmin)
								{
									//armazena o valor inserido pelo cliente
									uint16_t pump;
									retorno_socket= cliente.MyConexion.read_uint16(pump, SUP_TIMEOUT);
									if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o valor do usuário para alterar a bomba\n";
									
									SupServidor::setPumpInput(pump);
									//A BOMBA FOI ALTERADA COM SUCESSO
									cliente.MyConexion.write_uint16(SupCommands::CMD_OK);
								}
								else
								{
									cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
									throw "Erro: usuário não é administrador\n";//	ERRO NA ALTERAÇÃO DO VALOR DA BOMBA. USUARIO NÃO TEM PERMISSÃO
								} 
								break;
							default:
								cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
								throw "Comando invalido";
								break;
						}
			
						teve_atividade = fila_sockets.had_activity(MyServerSocket);

						if(teve_atividade)	// Erros na conexao de cliente: fecha socket temporario ou desconecta novo cliente
						{
							retorno_socket = MyServerSocket.accept(socket_temp); //armazena o status do accept da conexao temporaria com socket
							if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao aceitar conexao";
							
								//leh o comando
								retorno_socket=socket_temp.read_uint16(cmd);	//armazena o status do read do comando cmd
								if(retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o comando";

								//testa o comando
								if(cmd!=SupCommands::CMD_LOGIN) throw "Comando invalido";
								
								//leh o login do usuario que deseja se conectar
								retorno_socket=socket_temp.read_string(login, SUP_TIMEOUT);	//armazena o status do read do usuario
								if (retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o usuario";

								//leh a senha que o usuario deseja usar
								retorno_socket=socket_temp.read_string(senha, SUP_TIMEOUT);	//armazena o status do read da senha
								if (retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler a senha";

								retorno_socket=socket_temp.read_string(adm, SUP_TIMEOUT);	//armazena o status do read do admin	
								if (retorno_socket != mysocket_status::SOCK_OK) throw "Erro ao ler o status de admin";
								//tratando a variável adm para converter para bool
								transform(adm.begin(), adm.end(), adm.begin(), ::toupper);
								
								//verificando se o usuário digitou "S" ou "N" que são as opções válidas para determinar a variável admin
								if(adm=="S") admin = true;
								else admin = false;

								if(addUser(login,senha,admin))
								{
									if(admin) cliente.MyConexion.write_uint16(SupCommands::CMD_ADMIN_OK);
									else cliente.MyConexion.write_uint16(SupCommands::CMD_OK);
									fila_sockets.include(socket_temp);
									socket_temp.write_uint16(SupCommands::CMD_OK);
								}
								else{
									cliente.MyConexion.write_uint16(SupCommands::CMD_ERROR);
									throw "Erro ao adicionar o usuário\n";
								}
								//this->addUser(usuario, password, admin);							
						}// fim if(teve_atividade)
					}// fim if(fila_sockets.had_activity(cliente.MyConexion))
				}// fim for()
		}// fim switch (wait_read_result)

    }// fim try - Erros mais graves que encerram o servidor
    
	catch(const char* err)  // Erros mais graves que encerram o servidor
    {
      cerr << "Erro no servidor: " << err << endl;

      // Sai do while e encerra a thread
      server_on = false;

      // Fecha todos os sockets dos clientes
      for (auto& U : LU) U.close();
      // Fecha o socket de conexoes
      MyServerSocket.close();

      // Os tanques continuam funcionando

    } // fim catch - Erros mais graves que encerram o servidor
  } // fim while (server_on)
} // fim thr_server_main