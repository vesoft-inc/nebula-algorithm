# Nebula Graph related parameters

To import data, you must set parameters for Nebula Graph. This table lists all the Nebula Graph related parameters. For more information, see the [examples](../use-exchange/ex-ug-import-steps.md).

| Parameters | Default | Data Type | Required? | Description |
| :--- | :--- | :--- | :--- | :--- |
| `nebula.address.graph` | None | `list[string]` | Yes | Specifies the addresses and ports used by the Graph Service of Nebula Graph. Multiple addresses must be separated with commas. In the format of `"ip1:port","ip2:port","ip3:port"`.  |
| `nebula.address.meta` | None | `list[string]` | Yes | Specifies the addresses and ports used by the Meta Service of Nebula Graph. Multiple addresses must be separated with commas. In the format of `"ip1:port","ip2:port","ip3:port"`. |
| `nebula.user` | `user` | `string` | Yes | Specifies an account of Nebula Graph. The default value is `user`. If authentication is enabled in Nebula Graph: <br />- If no account is created, use `root`.<br />- If a specified account is created and given the write permission to a graph space, you can use this account. |
| `nebula.pswd` | `password` | `string` | Yes | Specifies the password of the specified account. The default password for the `user` account is `password`. If authentication is enabled in Nebula Graph: <br />- For the `root` account, use `nebula`. <br />- For another account, use the specified password. |
| `nebula.space` | None | `string` | Yes | Specifies the name of the graph space to import data. |
| `nebula.connection.timeout` | 3000 | `int` | No | Specifies the period of timeout for Thrift connection. Unit: ms. |
| `nebula.connection.retry` | 3 | `int` | No | Specifies the number of retries for Thrift connection. |
| `nebula.execution.retry` | 3 | `int` | No | Specifies the number of execution retries of an nGQL statements |
| `nebula.error.max` | 32 | `int` | No | Specifies the maximum number of failures during the import process. When the specified number of failures occur, the submitted Spark job stops automatically.  |
| `nebula.error.output` | None | `string` | Yes | Specifies a logging directory on the Nebula Graph cluster for the error message. |
