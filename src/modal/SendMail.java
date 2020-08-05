package modal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class SendMail {
	public static String CURRENT_DATE = java.time.LocalDate.now().toString();
	public static String CURRENT_TIME = java.time.LocalTime.now().toString().substring(0,
			java.time.LocalTime.now().toString().indexOf("."));
	public static String local_path = "C:\\WAREHOUSE\\LOGS_MAIL\\";
	static BufferedWriter bffWriter = null;
	static FileWriter fWriter = null;
	public static boolean AUTO_FLUSH = true;

	public static void setAUTO_FLUSH(boolean aUTO_FLUSH) {
		AUTO_FLUSH = aUTO_FLUSH;
	}
	

	public static void setBffWriter(BufferedWriter bffWriter) {
		SendMail.bffWriter = bffWriter;
	}


	public static void setfWriter(FileWriter fWriter) {
		SendMail.fWriter = fWriter;
	}


	public static void flushData() {
		try {
			if (bffWriter != null) {
				bffWriter.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			try {
				bffWriter.close();
			} catch (IOException e1) {
			}
		} finally {
			AUTO_FLUSH = true;
			fWriter = null;
			bffWriter = null;
		}
	}

	public static void writeLogsToLocalFile(String content) {
		File file = new File(local_path + CURRENT_DATE + ".txt");
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			if (fWriter == null)
				fWriter = new FileWriter(file, true);
			if (bffWriter == null)
				bffWriter = new BufferedWriter(fWriter);
			bffWriter.write(content);
			bffWriter.newLine();
			if (AUTO_FLUSH) {
				bffWriter.close();
				fWriter = null;
				bffWriter = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void sendMail() {
		// 1) get the session object
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.host", MailConfig.HOST_NAME);
		props.put("mail.smtp.socketFactory.port", MailConfig.SSL_PORT);
		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.port", MailConfig.SSL_PORT);

		Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(MailConfig.APP_EMAIL, MailConfig.APP_PASSWORD);
			}
		});

		// 2) compose message
		try {
			MimeMessage message = new MimeMessage(session);
			message.setFrom(new InternetAddress(MailConfig.APP_EMAIL));
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(MailConfig.RECEIVE_EMAIL));
			message.setSubject("DATA WAREHOUSE REPORT - " + CURRENT_DATE);

			// 3) create MimeBodyPart object and set your message text
			BodyPart messageBodyPart1 = new MimeBodyPart();
			messageBodyPart1.setText("WELLCOME TO TEAM 11");

			// 4) create new MimeBodyPart object and set DataHandler object to this object
			MimeBodyPart messageBodyPart2 = new MimeBodyPart();

			String filename = local_path + CURRENT_DATE + ".txt";
			DataSource source = new FileDataSource(filename);
			messageBodyPart2.setDataHandler(new DataHandler(source));
			messageBodyPart2.setFileName(filename);

			// 5) create Multipart object and add MimeBodyPart objects to this object
			Multipart multipart = new MimeMultipart();
			multipart.addBodyPart(messageBodyPart1);
			multipart.addBodyPart(messageBodyPart2);

			// 6) set the multiplart object to the message object
			message.setContent(multipart);

			// 7) send message
			Transport.send(message);

		} catch (MessagingException ex) {
			ex.printStackTrace();
		}
	}

}
