/*
* CS 6378.002: Advanced Operating Systems
* Fall 2013 | Project 2
* Checkpointing and Recovery
* Kiran Gavali, Ashish Kumar Dwivedi
*/

*************
* COMPILING *
*************

1. Unzip the contents of the archive: AOS_project2_KiranGavali_final.zip
   It will create the following directories/files:
   AOS/src: contains the source files
   AOS/bin: contains the binaries
   AOS/aos_project2_kxg121530.sh: Script to clear previous binaries and compile again
2. Execute the following commands:
   cd AOS/
   chmod +x aos_project2_kxg121530.sh
   ./aos_project2_kxg121530.sh
3. At this point, the new binaries should be placed in the bin folder.

***********************
* RUNNING THE PROGRAM *
***********************

1. Make sure you are in the unzipped AOS directory.
2. The default configuration file is in the AOS/src folder: AOS/src/config.txt
3. Make required modifications to the configuration file.
4. cd to the bin folder: cd AOS/bin.
5. From the bin directory execute the following command:
   java startup.Node <NodeID> <Path of config file> <Path of directory where log is to be placed>
   Eg: java startup.Node 0 ../src/config.txt ./
6. This will start the program on the system. 
7. The program has to be started on all the nodes mentioned in the config file. Only when all nodes are up
   the nodes will make connections and start execution.

**********
* RESULT *
**********

1. The program progress and results will be displayed on the terminal.
2. The latest 2 permanent checkpoints are stored in binary form in the log directory specified while running the program.
   The names of the checkpoints are of form <nodeID>_1.ckpt and <nodeID>_2.ckpt [Eg: 0_1.ckpt, 0_2.ckpt]. 
3. After all application messages are processed, the system will quiesce i.e. nodes will try to take local checkpoints
   at their specific intervals but won't because there would be no cohorts. The node specified to be failed will keep 
   failing at specified intervals and rollback to the latest checkpoint which will remain the same in the quiescent state.

*************************
* VERIFCATION *
*************************

1. Since the protocol and logs are complex, we haven't programmed automated termination since that would involve doing
   convergecast to detect termination, which is out of scope of the project.
2. To verify that the latest checkpoints are pairwise consistent, we have provided a utility which reads the binary
   of latest checkpoint and displays on the screen.
3. The checkpoint consists the following info:
	- Timestamp when the checkpoint was taken
	- Last Label Received (LLR) vector
	- Last Label Sent (LLS) vector
	- First Label Sent (FLS) vector
	- Application object 
4. To verify consistency of latest checkpoint, from the bin directory run the following command:
	java common.Utility <nodeID>  
	This will print the latest checkpoint for specified node. Example checkpoint is shown below:
	kiiranh@gavz:~/workspace/AOSCheckPointAndRecovery/bin$ ls
	0_1.ckpt  2_1.ckpt  4_1.ckpt  application   config_3.txt  service
	0_2.ckpt  2_2.ckpt  4_2.ckpt  common        config_4.txt  startup
	1_1.ckpt  3_1.ckpt  5_1.ckpt  config_1.txt  config.txt
	1_2.ckpt  3_2.ckpt  5_2.ckpt  config_2.txt  model
	kiiranh@gavz:~/workspace/AOSCheckPointAndRecovery/bin$ java common.Utility 3
	LAST PERMANENT CHECKPOINT FOR NODE 3
	TimeStamp: 40
	isPermanent: true
	--- Last Label Received ---
	Node 4: 9
	Node 5: 9
	--- Last Label Sent ---
	Node 4: 10
	Node 5: 10
	--- First Label Sent ---
	Node 4: 10
	Node 5: 10
	Application Object: Z**ZL41T1N1
	kiiranh@gavz:~/workspace/AOSCheckPointAndRecovery/bin$ 
5. For all nodes i,j: LLR(j) at i <= LLS(i) at j

