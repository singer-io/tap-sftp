# Changelog

## 1.2.2
* Dependency upgrades [#49](https://github.com/singer-io/tap-sftp/pull/49)

## 1.2.1
  * Removes deprecated dependency terminaltables [#47](https://github.com/singer-io/tap-sftp/pull/47)

## 1.2.0
  * Support Multiple Encoding Formats [#36](https://github.com/singer-io/tap-sftp/pull/44)

## 1.1.2
  * Request Timeout Implementation [#36](https://github.com/singer-io/tap-sftp/pull/36)

## 1.1.1
  * Skip unreadable/permission denied files [#32](https://github.com/singer-io/tap-sftp/pull/32)

## 1.1.0
  * Bump singer-encodings version [#33](https://github.com/singer-io/tap-sftp/pull/33)
  * Increased tap-tester coverage  [#31](https://github.com/singer-io/tap-sftp/pull/31)
  * Added support for returning files in sorted manner [#29](https://github.com/singer-io/tap-sftp/pull/29)

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
