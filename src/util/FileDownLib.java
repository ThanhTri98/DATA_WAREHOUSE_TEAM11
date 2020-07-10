package util;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class FileDownLib {
	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	//regex_NotMatch: Là những file đã tồn tại trong table log (không down lại)
		public static int fileDownload(String hostname, int port, String user_name, String password, String remote_Path,
				String local_Path, String regex_Match,String regex_NotMatch) {
			CkSsh ssh = new CkSsh();
			CkGlobal ck = new CkGlobal();
			ck.UnlockBundle("TEAM 11");
			boolean success = ssh.Connect(hostname, port);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				return -1;
			}
			ssh.put_IdleTimeoutMs(5000);
			success = ssh.AuthenticatePw(user_name, password);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				return -1;
			}
			CkScp scp = new CkScp();

			success = scp.UseSsh(ssh);
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return -1;
			}
			scp.put_SyncMustMatch(regex_Match);
			scp.put_SyncMustNotMatch(regex_NotMatch);
			success = scp.SyncTreeDownload(remote_Path, local_Path, 6, false);
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return -1;
			}
			ssh.Disconnect();
			return 0;
		}
		public static void main(String[] args) {
			System.out.println("java.library.path :" + System.getProperty("java.library.path"));
		}

}
