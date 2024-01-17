const nodemailer = require('nodemailer')
const inlineBase64 = require('nodemailer-plugin-inline-base64')
const QRCode = require('qrcode')

const welcomeMail = (token, email, namaoutlet, fullName) => {
  return new Promise((resolve, reject) => {
    QRCode.toDataURL(token, { width: 480 })
      .then(imageurl => {
        const transporter = nodemailer.createTransport({
          host: 'smtp.zoho.com',
          port: 465,
          secure: true,
          auth: {
            user: 'cs@homansystem.com',
            pass: 'TBNvfwpCNnbc'
          }
        });
        transporter.use('compile', inlineBase64({ cidPrefix: 'qrCode_ ' }))
        transporter.sendMail({
          from: `"${process.env.email_name}" <${process.env.email_send}>`,
          to: email,
          subject: `Selamat! Outlet Anda: ${namaoutlet} telah berhasil aktif`,
          html: `
            <p>Dengan Hormat Bapak/Ibu ${fullName},</p>
            <br/>
            <p>Outlet Anda <b>${namaoutlet}</b> telah berhasil diaktivasi. Berikut kami lampirkan informasi untuk login ke outlet Anda via QR Code atau token</p>
            <p>Token: <b>${token}</b></p>
            <p><img src="${imageurl}" style="width:480px;height:480px;" /></p>
            <p>Untuk login ke head office dengan link <a href="https://portal.ipos5.co.id/">https://portal.ipos5.co.id/<a/>, anda diharuskan melakukan registrasi akun terlebih dahulu, agar dapat menggunakan akun head office. Jika sudah mempunyai akun, maka anda tinggal memasukan token <b>${token}</b>.</p>
            <p>Untuk login ke laporan anda dapat membuka link <a href="https://laporan.ipos5.co.id/">https://laporan.ipos5.co.id/<a/>dengan username dan password yang sama dengan yang anda buatkan di head office(<a href="https://portal.ipos5.co.id/">https://portal.ipos5.co.id/<a/>) sebelumnya.</p>
            <p>Bila Anda memiliki pertanyaan atau kendala yang dihadapi saat menggunakan ${process.env.email_name}, Anda bisa menghubungi Layanan Customer Service kami via e-mail ${process.env.email_cs} ataupun WhatsApp kami: ${process.env.no_wa}</p>
            <p>Atas perhatian Bapak/Ibu ${fullName}, kami ucapkan terima kasih.</p>
            <br/><br/>
            <p>Hormat Kami</p>
            <br/><br/><br/><br/>
            <p>${process.env.email_name}</p>
            `
        }).then((info) => {
          return (resolve({
            success: true,
            outlet: namaoutlet,
            email: info.messageId
          }))
        }).catch(err => reject(err))
      })
      .catch(err => reject(err))
  })
}

const deleteMail = (email, namaoutlet, fullName) => {
  return new Promise((resolve, reject) => {
    const transporter = nodemailer.createTransport({
      host: 'smtp.zoho.com',
      port: 465,
      secure: true,
      auth: {
        user: 'cs@homansystem.com',
        pass: 'TBNvfwpCNnbc'
      }
    });
    transporter.sendMail({
      from: `"${process.env.email_name}" <${process.env.email_send}>`,
      to: email,
      subject: `Outlet Anda: ${namaoutlet} telah dinonaktifkan`,
      html: `
            <p>Dengan Hormat Bapak/Ibu ${fullName},</p>
            <br/>
            <p>Kami dengan sedih menyatakan bahwa outlet Anda <b>${namaoutlet}</b> telah dinonaktifkan.</p>
            <p>Bila Anda memiliki pertanyaan atau kendala yang dihadapi saat menggunakan ${process.env.email_name}, Anda bisa menghubungi Customer Service kami via e-mail ${process.env.email_cs} ataupun WhatsApp kami: ${process.env.no_wa}</p>
            <p>Atas perhatian Bapak/Ibu ${fullName}, kami ucapkan terima kasih.</p>
            <br/><br/>
            <p>Hormat Kami</p>
            <br/><br/><br/><br/>
            <p>${process.env.email_name}</p>
            `
    }).then((info) => {
      return (resolve({
        success: true,
        outlet: namaoutlet,
        email: info.messageId
      }))
    }).catch(err => reject(err))
  })
}

exports.welcomeMail = welcomeMail;
exports.deleteMail = deleteMail;