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

	public static void fileDownload(String hostname, int port, String user_name, String password, String remote_Path,
			String local_Path, String regex_Match) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("TEAM 11");
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw(user_name, password);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		scp.put_SyncMustMatch(regex_Match);
		success = scp.SyncTreeDownload(remote_Path, local_Path, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		ssh.Disconnect();
	}
}
