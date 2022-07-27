package sqltx

import "fmt"

type (
	// 最終処理で行った処理（コミット，ロールバック）を定義します．
	FinallyCommand string

	// 最終処理で行うコミット又はロールバックで失敗した場合に返却されます．
	ErrFinally struct {
		Finally     FinallyCommand
		InternalErr error
	}
)

var (
	FinallyCommandCommit   = "commit"
	FinallyCommandRollback = "rollback"
)

// エラー内容を出力します．
func (e ErrFinally) Error() string {
	return fmt.Sprintf("failed to execute final command (command: %s): %s", string(e.Finally), e.InternalErr.Error())
}
