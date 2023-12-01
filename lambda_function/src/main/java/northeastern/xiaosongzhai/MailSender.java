package northeastern.xiaosongzhai;

import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

import java.util.Properties;

/**
 * @Author: Xiaosong Zhai
 * @date: 2023/11/30 19:58
 * @Description: Mail sender
 */
public class MailSender {

    private static final String SUBJECT = "The status of your submission download";
    private static final String USERNAME = "demo.mrjello.me";
    private static final String Host = "smtp.mandrillapp.com";
    private static final String Port = "587";
    private static final String FROM = "zxs@demo.mrjello.me";

    /**
     * send mail to user
     * @param userEmail userEmail
     */
    public static void sendMail(String apiKay,String userEmail, String content) {

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", Host);
        props.put("mail.smtp.port", Port);
        //create the Session object
        Authenticator authenticator = new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(USERNAME, apiKay);
            }
        };
        Session session = Session.getInstance(props, authenticator);
        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(FROM));
            message.setRecipient(Message.RecipientType.TO, new InternetAddress(userEmail));
            message.setSubject(SUBJECT);
            message.setText(content);
            Transport.send(message);
            System.out.println("Email Message Sent Successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args){
//        String apiKay = "md-I0Fu5zDQVE7oIfOH9gxaPg";
//        sendMailSuccess(apiKay, "zhai.xia@northeastern.edu");
//    }

}
