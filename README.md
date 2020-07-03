# mq
 消息队列
Handler 如下：
```
	msgConsume := func() HandlerMq {
		return func(ctx context.Context, msg []byte) error {
			s := Student{}
			err := json.Unmarshal(msg, &s)
			if err != nil {
				return err
			}
			fmt.Println(s)
			insert(string(msg), time.Now().Unix())
			return nil
		}
	}()
 ```
