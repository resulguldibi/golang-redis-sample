package main


import(

"github.com/go-redis/redis"
"fmt"
"time"
)


func main(){

	//testRedisCLient()
	client :=getRedisClient()

	setItemToRedis(client,"resul",123,time.Second * 100)

	val,err := getItemFromRedis(client,"resul")

	if err != nil{
		fmt.Println(err)
	}else{
		fmt.Printf("key : %s, value :  %v\n","resul",val)
	}


	//create command
	cmd := createCommand(client,"get","resul")

	res,err := cmd.Result()

	if err != nil{
		fmt.Println(err)
	}else{
		fmt.Println(res)
	}
	// create command


	//RPush - LPop func
	fmt.Println("RPush-LPop func")
	if err := client.RPush("queue","message1","message2","message3").Err(); err != nil{
		fmt.Println(err)
	}

	result1,err := client.LPop("queue").Result()
	if  err != nil{
		fmt.Println(err)
	}

	fmt.Println(result1)

	result2,err := client.LPop("queue").Result()
	if  err != nil{
		fmt.Println(err)
	}

	fmt.Println(result2)

	result3,err := client.LPop("queue").Result()
	if  err != nil{
		fmt.Println(err)
	}

	fmt.Println(result3)

	//RPush func

	//RPush-BLPop func
	fmt.Println("RPush-BLPop func")
	if err := client.RPush("queue","message1","message2","message3").Err(); err != nil{
		fmt.Println(err)
	}

	result4,err := client.BLPop(1*time.Second,"queue").Result()
	if  err != nil{
		fmt.Println(err)
	}

	fmt.Println(result4)

	result5,err := client.BLPop(1*time.Second,"queue").Result()
	if  err != nil{
		fmt.Println(err)
	}

	fmt.Println(result5)

	result6,err := client.BLPop(1*time.Second,"queue").Result()
	if  err != nil{
		fmt.Println(err)
	}

	fmt.Println(result6)

	//RPush func

	//Incr func

	fmt.Println("Incr")
	result, err := client.Incr("counter").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(result)

	//Client-Pipeline
	fmt.Println("Client-Pipeline")
	pipe := client.Pipeline()

	incr := pipe.Incr("pipeline_counter")
	pipe.Expire("pipeline_counter", time.Hour)

	// Execute
	//
	//     INCR pipeline_counter
	//     EXPIRE pipeline_counts 3600
	//
	// using one client-server roundtrip.
	_, err = pipe.Exec()
	fmt.Println(incr.Val(), err)


	//Client-Pipelined
	fmt.Println("Client-Pipelined")
	var incr2 *redis.IntCmd
	_, err = client.Pipelined(func(pipe redis.Pipeliner) error {
		incr2 = pipe.Incr("pipelined_counter")
		pipe.Expire("pipelined_counter", time.Hour)
		return nil
	})
	fmt.Println(incr2.Val(), err)


	//Client-Scan
	fmt.Println("Client-Scan")


	client.FlushDB()
	for i := 0; i < 33; i++ {
		err := client.Set(fmt.Sprintf("key%d", i), "value", 0).Err()
		if err != nil {
			panic(err)
		}
	}

	var cursor uint64
	var n int
	for {
		var keys []string
		var err error
		keys, cursor, err = client.Scan(cursor, "", 10).Result()
		if err != nil {
			panic(err)
		}

		fmt.Printf("cursor : %v\n",cursor)
		fmt.Printf("keys : %v\n",keys)
		n += len(keys)
		if cursor == 0 {
			break
		}
	}

	fmt.Printf("found %d keys\n", n)
	
}

func getRedisClient() *redis.Client{
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func testRedisCLient(){
	client :=getRedisClient()
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
}

func setItemToRedis(client *redis.Client, key string, value interface{},duration time.Duration) error{
	return client.Set(key,value,duration).Err()
}

func getItemFromRedis(client *redis.Client, key string) (interface{},error){
	val, err := client.Get(key).Result()

	if err == redis.Nil {
		 return nil,fmt.Errorf("%s does not exist",key)
	}else if err != nil {
		return nil,err
	}

	return val,nil
}

func createCommand(client *redis.Client,args ...interface{}) *redis.StringCmd {
	cmd := redis.NewStringCmd(args...)
    client.Process(cmd)
    return cmd
}